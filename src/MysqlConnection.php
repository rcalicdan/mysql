<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\Handlers\HandshakeHandler;
use Hibla\MysqlClient\Handlers\QueryHandler;
use Hibla\MysqlClient\Handlers\PingHandler;
use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\ValueObjects\CommandRequest;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connector;
use Hibla\Socket\Interfaces\ConnectorInterface;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Packet\UncompressedPacketReader;

class MysqlConnection implements ConnectionInterface
{
    private ConnectionParams $params;
    private ConnectionState $state = ConnectionState::DISCONNECTED;
    private ?SocketConnection $socket = null;
    private ?UncompressedPacketReader $packetReader = null;
    private ?HandshakeHandler $handshakeHandler = null;
    private ?QueryHandler $queryHandler = null;
    private ?PingHandler $pingHandler = null;
    private ?Promise $connectPromise = null;
    private bool $isClosingError = false;
    private bool $isUserClosing = false;

    /** @var \SplQueue<CommandRequest> */
    private \SplQueue $commandQueue;
    private ?CommandRequest $currentCommand = null;

    public function __construct(
        ConnectionParams|array|string $config,
        private readonly ?ConnectorInterface $connector = null
    ) {
        $this->params = match (true) {
            $config instanceof ConnectionParams => $config,
            \is_array($config) => ConnectionParams::fromArray($config),
            \is_string($config) => ConnectionParams::fromUri($config),
        };
        $this->commandQueue = new \SplQueue();
    }

    /**
     * @return PromiseInterface<self>
     */
    public static function create(ConnectionParams|array|string $config, ?ConnectorInterface $connector = null): PromiseInterface
    {
        $connection = new self($config, $connector);
        return $connection->connect();
    }

    public function connect(): PromiseInterface
    {
        if ($this->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(new \LogicException('Connection is already active'));
        }

        $this->state = ConnectionState::CONNECTING;
        $this->connectPromise = new Promise();
        $this->isUserClosing = false;

        $connector = $this->connector ?? new Connector();
        $socketUri = \sprintf('tcp://%s:%d', $this->params->host, $this->params->port);

        $connector->connect($socketUri)->then(
            $this->handleSocketConnected(...),
            $this->handleConnectionError(...)
        );

        return $this->connectPromise;
    }

    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) return;

        $this->isUserClosing = true;
        $this->state = ConnectionState::CLOSED;

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }
    }

    public function getState(): ConnectionState
    {
        return $this->state;
    }

    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    public function isClosed(): bool
    {
        return $this->state === ConnectionState::CLOSED;
    }

    /**
     * @inheritDoc
     */
    public function execute(string $sql): PromiseInterface
    {
        return $this->query($sql);
    }

    /**
     * @inheritDoc
     */
    public function query(string $sql): PromiseInterface
    {
        $promise = new Promise();
        $this->commandQueue->enqueue(new CommandRequest(CommandRequest::TYPE_QUERY, $sql, $promise));
        $this->processNextCommand();
        return $promise;
    }

    public function ping(): PromiseInterface
    {
        $promise = new Promise();
        $this->commandQueue->enqueue(new CommandRequest(CommandRequest::TYPE_PING, '', $promise));
        $this->processNextCommand();
        return $promise;
    }

    private function handleSocketConnected(SocketConnection $socket): void
    {
        if ($this->state !== ConnectionState::CONNECTING) {
            $socket->close();
            return;
        }

        $this->socket = $socket;
        $this->packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $this->handshakeHandler = new HandshakeHandler($socket, $this->params);
        $this->queryHandler = new QueryHandler($socket, new CommandBuilder());

        $this->socket->on('data', $this->handleData(...));
        $this->socket->on('close', $this->handleSocketClose(...));
        $this->socket->on('error', $this->handleSocketError(...));

        $this->handshakeHandler->start($this->packetReader)->then(
            $this->handleHandshakeSuccess(...),
            $this->handleHandshakeError(...)
        );
    }

    private function handleHandshakeSuccess(int $nextSeqId): void
    {
        $this->state = ConnectionState::READY;

        if ($this->connectPromise) {
            $this->connectPromise->resolve($this);
            $this->connectPromise = null;
        }
        $this->processNextCommand();
    }

    private function finishCommand(): void
    {
        $this->state = ConnectionState::READY;
        $this->currentCommand = null;
        $this->processNextCommand();
    }

    private function handleData(string $chunk): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        try {
            $this->packetReader->append($chunk);

            while ($this->packetReader->hasPacket()) {

                $success = $this->packetReader->readPayload(function ($payloadReader, $length, $seq) {
                    match ($this->state) {
                        ConnectionState::CONNECTING => $this->handshakeHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::QUERYING   => $this->queryHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PINGING    => $this->pingHandler?->processPacket($payloadReader, $length, $seq),
                        default                     => null,
                    };
                });

                if (!$success) {
                    break;
                }
            }
        } catch (\Throwable $e) {
            $this->handleError($e);
        }
    }

    private function processNextCommand(): void
    {
        if ($this->state !== ConnectionState::READY || $this->currentCommand !== null || $this->commandQueue->isEmpty()) {
            return;
        }

        $this->currentCommand = $this->commandQueue->dequeue();

        if ($this->currentCommand->type === CommandRequest::TYPE_PING) {
            $this->state = ConnectionState::PINGING;

            $this->pingHandler ??= new PingHandler($this->socket);
            $this->pingHandler->start($this->currentCommand->promise);
        } else {
            $this->state = ConnectionState::QUERYING;
            $this->queryHandler->start($this->currentCommand->sql, $this->currentCommand->promise);
        }

        $this->currentCommand->promise->then(
            $this->finishCommand(...),
            $this->finishCommand(...)
        );
    }

    private function handleConnectionError(\Throwable $e): void
    {
        $this->handleError($e);
    }

    private function handleHandshakeError(\Throwable $e): void
    {
        $this->handleError($e);
    }

    private function handleSocketError(\Throwable $e): void
    {
        $this->handleError($e);
    }

    private function handleError(\Throwable $e): void
    {
        if ($this->isClosingError) return;

        $this->isClosingError = true;
        $this->state = ConnectionState::CLOSED;

        if ($this->connectPromise) {
            $this->connectPromise->reject($e);
            $this->connectPromise = null;
        }
        if ($this->currentCommand) {
            $this->currentCommand->promise->reject($e);
        }
        while (!$this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();
            $cmd->promise->reject(new \RuntimeException("Connection closed before execution", 0, $e));
        }
        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }
        $this->isClosingError = false;
    }

    private function handleSocketClose(): void
    {
        if ($this->isClosingError || $this->isUserClosing) return;

        $this->state = ConnectionState::CLOSED;

        $exception = new \RuntimeException("Connection closed unexpectedly");

        if ($this->connectPromise) {
            $this->connectPromise->reject($exception);
            $this->connectPromise = null;
        }

        if ($this->currentCommand) {
            $this->currentCommand->promise->reject($exception);
        }
    }
}
