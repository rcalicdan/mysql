<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\Handlers\ExecuteHandler;
use Hibla\MysqlClient\Handlers\HandshakeHandler;
use Hibla\MysqlClient\Handlers\PingHandler;
use Hibla\MysqlClient\Handlers\PrepareHandler;
use Hibla\MysqlClient\Handlers\QueryHandler;
use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\ValueObjects\CommandRequest;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\MysqlClient\ValueObjects\ExecuteResult;
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
    private readonly ConnectionParams $params;
    private ConnectionState $state = ConnectionState::DISCONNECTED;
    private ?SocketConnection $socket = null;
    private ?UncompressedPacketReader $packetReader = null;
    private ?HandshakeHandler $handshakeHandler = null;
    private ?QueryHandler $queryHandler = null;
    private ?PingHandler $pingHandler = null;
    private ?PrepareHandler $prepareHandler = null;
    private ?ExecuteHandler $executeHandler = null;
    private ?Promise $connectPromise = null;
    private bool $isClosingError = false;
    private bool $isUserClosing = false;
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
    public static function create(
        ConnectionParams|array|string $config,
        ?ConnectorInterface $connector = null
    ): PromiseInterface {
        $connection = new self($config, $connector);
        return $connection->connect();
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);
    }

    /**
     * {@inheritDoc}
     */
    public function execute(string $sql): PromiseInterface
    {
        return $this->query($sql);
    }

    /**
     * {@inheritDoc}
     */
    public function prepare(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PREPARE, $sql);
    }

    /**
     * {@inheritDoc}
     */
    public function ping(): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PING);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    public function getState(): ConnectionState
    {
        return $this->state;
    }

    /**
     * {@inheritDoc}
     */
    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    /**
     * {@inheritDoc}
     */
    public function isClosed(): bool
    {
        return $this->state === ConnectionState::CLOSED;
    }

    // ================================================================================================================
    // Internal API (Used by PreparedStatement)
    // ================================================================================================================

    /**
     * Internal: Called by PreparedStatement to execute bound parameters.
     * @return PromiseInterface<ExecuteResult|QueryResult>
     */
    public function executeStatement(PreparedStatement $stmt, array $params): PromiseInterface
    {
        return $this->enqueueCommand(
            CommandRequest::TYPE_EXECUTE,
            '',
            $params,
            $stmt->id,
            $stmt
        );
    }

    /**
     * Internal: Called by PreparedStatement to close itself.
     * @return PromiseInterface<void>
     */
    public function closeStatement(int $stmtId): PromiseInterface
    {
        return $this->enqueueCommand(
            CommandRequest::TYPE_CLOSE_STMT,
            '',
            [],
            $stmtId
        );
    }

    // ================================================================================================================
    // Private Logic (Queue & Handlers)
    // ================================================================================================================

    private function enqueueCommand(
        string $type,
        string $sql = '',
        array $params = [],
        int $stmtId = 0,
        mixed $context = null
    ): PromiseInterface {
        $promise = new Promise();
        $this->commandQueue->enqueue(new CommandRequest($type, $promise, $sql, $params, $stmtId, $context));
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

        // Initialize all Handlers
        $commandBuilder = new CommandBuilder();
        $this->handshakeHandler = new HandshakeHandler($socket, $this->params);
        $this->queryHandler = new QueryHandler($socket, $commandBuilder);
        $this->pingHandler = new PingHandler($socket);
        $this->prepareHandler = new PrepareHandler($this, $socket, $commandBuilder);
        $this->executeHandler = new ExecuteHandler($socket, $commandBuilder);

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

    private function processNextCommand(): void
    {
        if ($this->state !== ConnectionState::READY || $this->currentCommand !== null || $this->commandQueue->isEmpty()) {
            return;
        }

        $this->currentCommand = $this->commandQueue->dequeue();

        switch ($this->currentCommand->type) {
            case CommandRequest::TYPE_QUERY:
                $this->state = ConnectionState::QUERYING;
                $this->queryHandler->start($this->currentCommand->sql, $this->currentCommand->promise);
                break;

            case CommandRequest::TYPE_PING:
                $this->state = ConnectionState::PINGING;
                $this->pingHandler->start($this->currentCommand->promise);
                break;

            case CommandRequest::TYPE_PREPARE:
                $this->state = ConnectionState::PREPARING;
                $this->prepareHandler->start($this->currentCommand->sql, $this->currentCommand->promise);
                break;

            case CommandRequest::TYPE_EXECUTE:
                $this->state = ConnectionState::EXECUTING;
                /** @var PreparedStatement $stmt */
                $stmt = $this->currentCommand->context;
                $this->executeHandler->start(
                    $stmt->id,
                    $this->currentCommand->params,
                    $stmt->columnDefinitions,
                    $this->currentCommand->promise
                );
                break;

            case CommandRequest::TYPE_CLOSE_STMT:
                $this->sendClosePacket($this->currentCommand->statementId);
                $this->currentCommand->promise->resolve(null);
                $this->currentCommand = null;
                $this->processNextCommand();
                return;
        }

        $this->currentCommand->promise->then(
            fn() => $this->finishCommand(),
            fn() => $this->finishCommand()
        );
    }

    private function finishCommand(): void
    {
        $this->state = ConnectionState::READY;
        $this->currentCommand = null;
        $this->processNextCommand();
    }

    private function sendClosePacket(int $stmtId): void
    {
        $payload = chr(0x19) . pack('V', $stmtId);
        $header = substr(pack('V', 5), 0, 3) . chr(0);
        $this->socket->write($header . $payload);
    }

    private function handleData(string $chunk): void
    {
        if ($this->state === ConnectionState::CLOSED) return;

        try {
            $this->packetReader->append($chunk);

            while ($this->packetReader->hasPacket()) {
                $success = $this->packetReader->readPayload(function ($payloadReader, $length, $seq) {
                    match ($this->state) {
                        ConnectionState::CONNECTING =>
                        $this->handshakeHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::QUERYING =>
                        $this->queryHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PINGING =>
                        $this->pingHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PREPARING =>
                        $this->prepareHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::EXECUTING =>
                        $this->executeHandler?->processPacket($payloadReader, $length, $seq),
                        default => null
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