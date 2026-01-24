<?php

namespace Hibla\MysqlClient;

use Hibla\EventLoop\Loop;
use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\Enums\PacketMarker;
use Hibla\MysqlClient\Handlers\RequestQueueHandler;
use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\Protocols\PacketBuilder;
use Hibla\MysqlClient\Protocols\ResultSetParser;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\MysqlClient\ValueObjects\ErrPacket;
use Hibla\MysqlClient\ValueObjects\OkPacket;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connection as SocketConnection;
use Hibla\Socket\Internals\StreamEncryption;
use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeV10;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeV10Builder;
use Rcalicdan\MySQLBinaryProtocol\Packet\PacketReader;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class Connection implements ConnectionInterface
{
    private const int CLIENT_CAPABILITIES = 
        CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_LONG_PASSWORD
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_MULTI_RESULTS
        | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
        | CapabilityFlags::CLIENT_DEPRECATE_EOF
        | CapabilityFlags::CLIENT_SSL; 

    private ConnectionState $state = ConnectionState::DISCONNECTED;
    private PacketReader $packetReader;
    private PacketBuilder $packetBuilder;
    private ResultSetParser $resultParser;
    private RequestQueueHandler $requestQueue;
    private int $sequenceId = 0;
    private ?HandshakeV10 $handshake = null;
    private ?SocketConnection $stream = null;
    private ?Promise $authPromise = null;
    private bool $sslEnabled = false;
    private bool $sslRequested = false;

    public function __construct(
        private readonly ConnectionParams $params
    ) {
        $this->packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $this->packetBuilder = new PacketBuilder($this->params, self::CLIENT_CAPABILITIES);
        $this->resultParser = new ResultSetParser();
        $this->requestQueue = new RequestQueueHandler();
    }

    public function waitForAuthentication(): Promise
    {
        if ($this->authPromise === null) {
            $this->authPromise = new Promise();
        }
        
        return $this->authPromise;
    }

    public function attachStream(SocketConnection $stream): void
    {
        $this->stream = $stream;
        $this->state = ConnectionState::HANDSHAKING;
        
        $this->stream->on('data', function (string $data): void {
            $this->handleData($data);
        });
        
        $this->stream->on('close', function (): void {
            $this->handleDisconnect();
        });
        
        $this->stream->on('error', function (\Throwable $error): void {
            $this->handleError($error);
        });
    }

    public function query(string $sql): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(new \RuntimeException('Connection not ready'));
        }
        
        $promise = $this->requestQueue->enqueue();
        
        $packet = $this->packetBuilder->buildQueryPacket($sql);
        $this->sendPacket($packet);
        
        $this->state = ConnectionState::QUERYING;
        $this->resultParser->reset();
        
        return $promise;
    }

    public function prepare(string $sql): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(new \RuntimeException('Connection not ready'));
        }
        
        $promise = $this->requestQueue->enqueue();
        
        $packet = $this->packetBuilder->buildStmtPreparePacket($sql);
        $this->sendPacket($packet);
        
        $this->state = ConnectionState::QUERYING;
        
        return $promise;
    }

    public function close(): PromiseInterface
    {
        if ($this->state === ConnectionState::CLOSED || $this->state === ConnectionState::CLOSING) {
            return Promise::resolved(null);
        }
        
        $this->state = ConnectionState::CLOSING;
        
        $packet = $this->packetBuilder->buildQuitPacket();
        $this->sendPacket($packet);
        
        Loop::defer(function (): void {
            $this->stream?->close();
            $this->state = ConnectionState::CLOSED;
        });
        
        return Promise::resolved(null);
    }

    public function ping(): PromiseInterface
    {
        return Promise::rejected(new \RuntimeException('Ping not yet implemented'));
    }

    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    public function getState(): ConnectionState
    {
        return $this->state;
    }

    private function handleData(string $data): void
    {
        $this->packetReader->append($data);
        
        while ($this->packetReader->readPayload(fn(...$args) => $this->handlePayload(...$args))) {
            // Process all available packets
        }
    }

    private function handlePayload(PayloadReader $reader, int $length, int $sequence): void
    {
        $this->sequenceId = $sequence + 1;
        
        match ($this->state) {
            ConnectionState::HANDSHAKING => $this->handleHandshake($reader),
            ConnectionState::AUTHENTICATING => $this->handleAuthResponse($reader),
            ConnectionState::QUERYING => $this->handleQueryResponse($reader, $length),
            default => throw new \RuntimeException("Unexpected state: {$this->state->value}")
        };
    }

    private function handleHandshake(PayloadReader $reader): void
    {
        $parser = new HandshakeParser(
            new HandshakeV10Builder(),
            function (HandshakeV10 $handshake): void {
                $this->handshake = $handshake;
                
                if ($this->params->useSsl() 
                    && ($handshake->capabilities & CapabilityFlags::CLIENT_SSL)) {
                    $this->requestSsl();
                } else {
                    // No SSL - send auth directly
                    $this->sendAuthResponse();
                }
            }
        );
        
        $parser($reader);
    }

    private function requestSsl(): void
    {
        if ($this->handshake === null) {
            throw new \RuntimeException('No handshake received');
        }
        
        // Send SSL request packet
        $packet = $this->packetBuilder->buildSslRequest();
        $this->sendPacket($packet);
        
        $this->sslRequested = true;
        
        // Pause reading to perform TLS upgrade
        $this->stream->pause();
        
        // Upgrade to TLS
        $encryption = new StreamEncryption(isServer: false);
        
        $encryption->enable($this->stream)
            ->then(function (SocketConnection $secureStream): void {
                $this->sslEnabled = true;
                $this->stream->resume();
                
                // Now send authentication over encrypted connection
                $this->sendAuthResponse();
            })
            ->catch(function (\Throwable $error): void {
                if ($this->authPromise !== null) {
                    $this->authPromise->reject($error);
                }
                $this->stream?->close();
            });
    }

    private function sendAuthResponse(): void
    {
        if ($this->handshake === null) {
            throw new \RuntimeException('No handshake received');
        }
        
        $packet = $this->packetBuilder->buildHandshakeResponse($this->handshake->authData);
        $this->sendPacket($packet);
        
        $this->state = ConnectionState::AUTHENTICATING;
    }

    private function handleAuthResponse(PayloadReader $reader): void
    {
        $firstByte = \ord($reader->readFixedString(1));
        
        if ($firstByte === PacketMarker::OK->value) {
            // Authentication successful
            $this->state = ConnectionState::READY;
            
            if ($this->authPromise !== null) {
                $this->authPromise->resolve($this);
            }
            
        } elseif ($firstByte === PacketMarker::ERR->value) {
            // Authentication failed
            $error = ErrPacket::fromPayload($reader);
            
            if ($this->authPromise !== null) {
                $this->authPromise->reject($error);
            }
            
            $this->stream?->close();
            
        } elseif ($firstByte === 0x01) {
            // Auth continuation for caching_sha2_password
            $authContinue = \ord($reader->readFixedString(1));
            
            if ($authContinue === 0x03) {
                // Fast auth success - server has cached password
                $this->state = ConnectionState::READY;
                
                if ($this->authPromise !== null) {
                    $this->authPromise->resolve($this);
                }
            } elseif ($authContinue === 0x04) {
                // Server requests full password
                if ($this->sslEnabled) {
                    // Safe to send password over TLS
                    $this->sendClearTextPassword();
                } else {
                    // No SSL - cannot send password securely
                    $error = new \RuntimeException(
                        'Server requires full password but SSL is not enabled. ' .
                        'Enable SSL or use mysql_native_password authentication.'
                    );
                    
                    if ($this->authPromise !== null) {
                        $this->authPromise->reject($error);
                    }
                    
                    $this->stream?->close();
                }
            } else {
                $error = new \RuntimeException('Unknown auth continue: 0x' . dechex($authContinue));
                
                if ($this->authPromise !== null) {
                    $this->authPromise->reject($error);
                }
            }
            
        } else {
            $error = new \RuntimeException('Unexpected auth response: 0x' . dechex($firstByte));
            
            if ($this->authPromise !== null) {
                $this->authPromise->reject($error);
            }
        }
    }

    private function sendClearTextPassword(): void
    {
        if ($this->stream === null) {
            throw new \RuntimeException('Not connected');
        }
        
        // Send password as null-terminated string
        $password = $this->params->password . "\x00";
        $this->sendPacket($password);
        
        // Stay in AUTHENTICATING state to receive the OK packet
    }

    private function handleQueryResponse(PayloadReader $reader, int $length): void
    {
        $rawPayload = $reader->readFixedString($length);
        $firstByte = ord($rawPayload[0]);
        
        if ($firstByte === PacketMarker::OK->value) {
            $okPacket = OkPacket::fromPayload(
                (new \Rcalicdan\MySQLBinaryProtocol\Buffer\Reader\BufferPayloadReaderFactory())
                    ->createFromString($rawPayload)
            );
            
            $this->state = ConnectionState::READY;
            $this->requestQueue->resolve($okPacket);
            
        } elseif ($firstByte === PacketMarker::ERR->value) {
            $errPacket = ErrPacket::fromPayload(
                (new \Rcalicdan\MySQLBinaryProtocol\Buffer\Reader\BufferPayloadReaderFactory())
                    ->createFromString($rawPayload)
            );
            
            $this->state = ConnectionState::READY;
            $this->requestQueue->reject($errPacket);
            
        } else {
            // Result set - feed to parser
            $this->resultParser->processPayload($rawPayload);
            
            if ($this->resultParser->isComplete()) {
                $this->state = ConnectionState::READY;
                $this->requestQueue->resolve($this->resultParser->getResult());
            }
        }
    }

    private function sendPacket(string $payload): void
    {
        if ($this->stream === null) {
            throw new \RuntimeException('Not connected');
        }
        
        $length = \strlen($payload);
        
        // Build packet header (3 bytes length + 1 byte sequence)
        $header = pack('V', $length);
        $header = substr($header, 0, 3);
        $header .= \chr($this->sequenceId);
        
        $this->stream->write($header . $payload);
        $this->sequenceId++;
    }

    private function handleDisconnect(): void
    {
        $this->state = ConnectionState::CLOSED;
        $this->requestQueue->rejectAll(new \RuntimeException('Connection closed'));
        
        if ($this->authPromise !== null && !$this->authPromise->isSettled()) {
            $this->authPromise->reject(new \RuntimeException('Connection closed during authentication'));
        }
    }

    private function handleError(\Throwable $error): void
    {
        $this->requestQueue->reject($error);
        
        if ($this->authPromise !== null && !$this->authPromise->isSettled()) {
            $this->authPromise->reject($error);
        }
        
        $this->stream?->close();
    }
}