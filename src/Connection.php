<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Evenement\EventEmitter;
use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnectionInterface;
use Hibla\Socket\Connector;
use Rcalicdan\MySQLBinaryProtocol\Auth\AuthScrambler;
use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;
use Rcalicdan\MySQLBinaryProtocol\Constants\CharsetIdentifiers;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketWriterFactory;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeResponse41;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeV10;
use Rcalicdan\MySQLBinaryProtocol\Packet\PacketReader;
use Rcalicdan\MySQLBinaryProtocol\Packet\PacketWriter;

/**
 * MySQL Connection Implementation
 * 
 * Manages a single connection to a MySQL server with support for
 * authentication, query execution, and connection lifecycle.
 * 
 * @internal This class should not be instantiated directly. Use Client instead.
 */
class Connection extends EventEmitter implements ConnectionInterface
{
    private ConnectionState $state = ConnectionState::DISCONNECTED;
    
    private ?SocketConnectionInterface $socket = null;
    
    private PacketReader $packetReader;
    
    private PacketWriter $packetWriter;
    
    private CommandBuilder $commandBuilder;
    
    private int $sequenceNumber = 0;
    
    private ?HandshakeV10 $handshake = null;
    
    private int $capabilities = 0;
    
    /**
     * @var array<int, array{resolve: callable, reject: callable, parser: ResponseHandler}>
     */
    private array $pendingRequests = [];
    
    private int $nextRequestId = 0;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly ConnectionParams $params,
        private readonly ?Connector $connector = null
    ) {
        $packetReaderFactory = new DefaultPacketReaderFactory();
        $this->packetReader = $packetReaderFactory->createWithDefaultSettings();
        
        $packetWriterFactory = new DefaultPacketWriterFactory();
        $this->packetWriter = $packetWriterFactory->createWithDefaultSettings();
        
        $this->commandBuilder = new CommandBuilder();
        
        $this->setupCapabilities();
    }

    /**
     * {@inheritdoc}
     */
    public function connect(): PromiseInterface
    {
        if ($this->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(
                new \RuntimeException('Connection already established or in progress')
            );
        }

        $this->state = ConnectionState::CONNECTING;
        
        $connector = $this->connector ?? new Connector([
            'timeout' => $this->params->connectTimeout,
            'tls' => $this->params->useSsl() ? [
                'verify_peer' => $this->params->sslVerify,
                'cafile' => $this->params->sslCa,
                'local_cert' => $this->params->sslCert,
                'local_pk' => $this->params->sslKey,
            ] : false,
        ]);

        return $connector->connect("tcp://{$this->host}:{$this->port}")
            ->then(function (SocketConnectionInterface $socket) {
                $this->socket = $socket;
                $this->setupSocketHandlers();
                
                return $this->waitForHandshake();
            })
            ->then(function (HandshakeV10 $handshake) {
                $this->handshake = $handshake;
                
                return $this->authenticate();
            })
            ->then(function () {
                $this->state = ConnectionState::READY;
                $this->emit('ready');
                
                return $this;
            })
            ->catch(function (\Throwable $error) {
                $this->state = ConnectionState::DISCONNECTED;
                $this->close();
                
                throw $error;
            });
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->executeCommand($sql, expectRows: true);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql): PromiseInterface
    {
        return $this->executeCommand($sql, expectRows: false);
    }

    /**
     * {@inheritdoc}
     */
    public function ping(): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(
                new \RuntimeException("Cannot ping: connection is {$this->state->value}")
            );
        }

        $payload = $this->commandBuilder->buildPing();
        $packet = $this->packetWriter->write($payload, $this->sequenceNumber++);
        
        return $this->sendPacket($packet)
            ->then(function () {
                $parser = new ResponseHandler();
                
                return $this->waitForResponse($parser);
            })
            ->then(fn() => true);
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        $this->state = ConnectionState::CLOSING;

        if ($this->socket !== null && $this->socket->isWritable()) {
            try {
                $payload = $this->commandBuilder->buildQuit();
                $packet = $this->packetWriter->write($payload, $this->sequenceNumber++);
                $this->socket->write($packet);
            } catch (\Throwable $e) {
                // Ignore errors during quit
            }
        }

        if ($this->socket !== null) {
            $this->socket->close();
            $this->socket = null;
        }

        foreach ($this->pendingRequests as $request) {
            $request['reject'](new \RuntimeException('Connection closed'));
        }
        $this->pendingRequests = [];

        $this->state = ConnectionState::CLOSED;
        $this->emit('close');
    }

    /**
     * {@inheritdoc}
     */
    public function getState(): ConnectionState
    {
        return $this->state;
    }

    /**
     * {@inheritdoc}
     */
    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    /**
     * {@inheritdoc}
     */
    public function isClosed(): bool
    {
        return $this->state === ConnectionState::CLOSED;
    }

    /**
     * Execute a SQL command
     */
    private function executeCommand(string $sql, bool $expectRows): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(
                new \RuntimeException("Cannot execute: connection is {$this->state->value}")
            );
        }

        $this->state = ConnectionState::QUERYING;
        
        $payload = $this->commandBuilder->buildQuery($sql);
        $packet = $this->packetWriter->write($payload, $this->sequenceNumber++);
        
        return $this->sendPacket($packet)
            ->then(function () {
                $parser = new ResponseHandler();
                
                return $this->waitForResponse($parser);
            })
            ->then(function ($result) {
                $this->state = ConnectionState::READY;
                
                return $result;
            })
            ->catch(function (\Throwable $error) {
                $this->state = ConnectionState::READY;
                
                throw $error;
            });
    }

    private function setupSocketHandlers(): void
    {
        if ($this->socket === null) {
            return;
        }

        $this->socket->on('data', function (string $data) {
            $this->handleData($data);
        });

        $this->socket->on('error', function (\Throwable $error) {
            $this->emit('error', [$error]);
            $this->close();
        });

        $this->socket->on('close', function () {
            $this->close();
        });
    }

    private function handleData(string $data): void
    {
        $this->packetReader->append($data);

        while ($this->packetReader->readPayload(function ($payload, $length, $sequence) {
            $this->handlePacket($payload, $length, $sequence);
        })) {
            // Continue processing packets
        }
    }

    private function handlePacket($payload, int $length, int $sequence): void
    {
        $this->sequenceNumber = $sequence + 1;

        if ($this->state === ConnectionState::CONNECTING) {
            $this->emit('handshake-packet', [$payload, $length, $sequence]);
            return;
        }

        if ($this->state === ConnectionState::AUTHENTICATING) {
            $this->emit('auth-packet', [$payload, $length, $sequence]);
            return;
        }

        if (!empty($this->pendingRequests)) {
            $requestId = array_key_first($this->pendingRequests);
            $request = $this->pendingRequests[$requestId];
            
            $request['parser']->processPacket($payload, $length, $sequence);
            
            if ($request['parser']->isComplete()) {
                unset($this->pendingRequests[$requestId]);
                
                if ($request['parser']->hasError()) {
                    $request['reject']($request['parser']->getError());
                } else {
                    $request['resolve']($request['parser']->getResult());
                }
            }
        }
    }

    private function waitForHandshake(): PromiseInterface
    {
        return new Promise(function ($resolve, $reject) {
            $handler = null;
            
            $handler = function ($payload, $length, $sequence) use ($resolve, $reject, &$handler) {
                try {
                    $parser = new HandshakeParser();
                    $handshake = $parser->parse($payload, $length, $sequence);
                    
                    $this->removeListener('handshake-packet', $handler);
                    $resolve($handshake);
                } catch (\Throwable $e) {
                    $this->removeListener('handshake-packet', $handler);
                    $reject($e);
                }
            };
            
            $this->on('handshake-packet', $handler);
        });
    }

    private function authenticate(): PromiseInterface
    {
        if ($this->handshake === null) {
            return Promise::rejected(new \RuntimeException('No handshake received'));
        }

        $this->state = ConnectionState::AUTHENTICATING;

        $authResponse = match ($this->handshake->authPlugin) {
            'mysql_native_password' => AuthScrambler::scrambleNativePassword(
                $this->params->password,
                $this->handshake->authData
            ),
            'caching_sha2_password' => AuthScrambler::scrambleCachingSha2Password(
                $this->params->password,
                $this->handshake->authData
            ),
            default => throw new \RuntimeException(
                "Unsupported auth plugin: {$this->handshake->authPlugin}"
            ),
        };

        $responseBuilder = new HandshakeResponse41();
        $responsePayload = $responseBuilder->build(
            capabilities: $this->capabilities,
            charset: $this->getCharsetId(),
            username: $this->params->username,
            authResponse: $authResponse,
            database: $this->params->database,
            authPluginName: $this->handshake->authPlugin
        );

        $packet = $this->packetWriter->write($responsePayload, $this->sequenceNumber++);

        return $this->sendPacket($packet)
            ->then(function () {
                return $this->waitForAuthResponse();
            });
    }

    private function waitForAuthResponse(): PromiseInterface
    {
        return new Promise(function ($resolve, $reject) {
            $handler = null;
            
            $handler = function ($payload, $length, $sequence) use ($resolve, $reject, &$handler) {
                try {
                    $firstByte = $payload->readFixedInteger(1);
                    
                    if ($firstByte === 0x00) {
                        $this->removeListener('auth-packet', $handler);
                        $resolve(true);
                    } elseif ($firstByte === 0xFF) {
                        $errorParser = new \Rcalicdan\MySQLBinaryProtocol\Frame\Error\ErrPacketParser();
                        $error = $errorParser->parse($payload, $length, $sequence);
                        
                        $this->removeListener('auth-packet', $handler);
                        $reject(new \RuntimeException(
                            "Authentication failed: {$error->errorMessage} (Code: {$error->errorCode})"
                        ));
                    } else {
                        $this->removeListener('auth-packet', $handler);
                        $reject(new \RuntimeException(
                            "Unexpected auth response packet type: 0x" . dechex($firstByte)
                        ));
                    }
                } catch (\Throwable $e) {
                    $this->removeListener('auth-packet', $handler);
                    $reject($e);
                }
            };
            
            $this->on('auth-packet', $handler);
        });
    }

    private function sendPacket(string $packet): PromiseInterface
    {
        if ($this->socket === null || !$this->socket->isWritable()) {
            return Promise::rejected(new \RuntimeException('Socket is not writable'));
        }

        $this->socket->write($packet);
        
        return Promise::resolved();
    }

    private function waitForResponse(ResponseHandler $parser): PromiseInterface
    {
        return new Promise(function ($resolve, $reject) use ($parser) {
            $requestId = $this->nextRequestId++;
            
            $this->pendingRequests[$requestId] = [
                'resolve' => $resolve,
                'reject' => $reject,
                'parser' => $parser,
            ];
        });
    }

    private function setupCapabilities(): void
    {
        $this->capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS;

        if ($this->params->hasDatabase()) {
            $this->capabilities |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
        }
    }

    private function getCharsetId(): int
    {
        return match (strtolower($this->params->charset)) {
            'utf8mb4' => CharsetIdentifiers::UTF8MB4,
            'utf8' => CharsetIdentifiers::UTF8,
            'latin1' => CharsetIdentifiers::LATIN1,
            default => CharsetIdentifiers::UTF8MB4,
        };
    }
}