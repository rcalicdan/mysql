<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Internals;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\Enums\IsolationLevel;
use Hibla\MysqlClient\Handlers\ExecuteHandler;
use Hibla\MysqlClient\Handlers\HandshakeHandler;
use Hibla\MysqlClient\Handlers\PingHandler;
use Hibla\MysqlClient\Handlers\PrepareHandler;
use Hibla\MysqlClient\Handlers\QueryHandler;
use Hibla\MysqlClient\ValueObjects\CommandRequest;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\MysqlClient\ValueObjects\ExecuteStreamContext;
use Hibla\MysqlClient\ValueObjects\StreamContext;
use Hibla\MysqlClient\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connector;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Hibla\Socket\Interfaces\ConnectorInterface;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Packet\UncompressedPacketReader;

/**
 * @internal Used internally by the connection pool
 */
class Connection
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
     * Establish connection to the MySQL server.
     *
     * @return PromiseInterface<self>
     */
    public function connect(): PromiseInterface
    {
        if ($this->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(new \LogicException('Connection is already active'));
        }

        $this->state = ConnectionState::CONNECTING;
        $this->connectPromise = new Promise();
        $this->isUserClosing = false;

        $connector = $this->connector ?? new Connector([
            'tcp' => true,
            'tls' => false,
            'unix' => false,
            'dns' => true,
            'happy_eyeballs' => false,
        ]);

        $socketUri = \sprintf('tcp://%s:%d', $this->params->host, $this->params->port);

        $connector->connect($socketUri)->then(
            $this->handleSocketConnected(...),
            $this->handleConnectionError(...)
        );

        return $this->connectPromise;
    }

    /**
     * Execute a SELECT query and return all rows buffered in memory.
     *
     * Use this for small to medium result sets. For large result sets
     * that may exceed available memory, use streamQuery() instead.
     *
     * @param string $sql The SQL query to execute
     * @return PromiseInterface<QueryResult>
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);
    }

    /**
     * Execute a SQL statement (INSERT, UPDATE, DELETE, etc.).
     *
     * @param string $sql The SQL command to execute
     * @return PromiseInterface<ExecuteResult>
     */
    public function execute(string $sql): PromiseInterface
    {
        return $this->query($sql);
    }

    /**
     * Stream a SELECT query row-by-row without buffering in memory.
     * 
     * @param string $sql The SQL SELECT query to execute
     * @param callable(array): void $onRow Callback invoked for each row
     * @param callable(StreamStats): void|null $onComplete Optional callback when streaming completes
     * @param callable(\Throwable): void|null $onError Optional callback for error handling
     * @return PromiseInterface<StreamStats> Resolves with streaming statistics
     */
    public function streamQuery(
        string $sql,
        callable $onRow,
        ?callable $onComplete = null,
        ?callable $onError = null
    ): PromiseInterface {
        $context = new StreamContext($onRow, $onComplete, $onError);
        return $this->enqueueCommand(CommandRequest::TYPE_STREAM_QUERY, $sql, context: $context);
    }

    /**
     * Begin a transaction.
     *
     * @param IsolationLevel|null $isolationLevel Optional isolation level for this transaction
     * @return PromiseInterface<Transaction>
     */
    public function beginTransaction(?IsolationLevel $isolationLevel = null): PromiseInterface
    {
        if ($isolationLevel === null) {
            return $this->query('START TRANSACTION')->then(
                fn() => new Transaction($this)
            );
        }

        return $this->query("SET TRANSACTION ISOLATION LEVEL {$isolationLevel->value}")
            ->then(fn() => $this->query('START TRANSACTION'))
            ->then(fn() => new Transaction($this))
        ;
    }

    /**
     * Prepare a SQL statement for execution.
     *
     * @param string $sql The SQL statement with placeholders (?)
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PREPARE, $sql);
    }

    /**
     * Ping the server to check if connection is alive.
     *
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PING);
    }

    /**
     * Close the connection gracefully.
     */
    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        $this->isUserClosing = true;
        $this->state = ConnectionState::CLOSED;

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        $this->packetReader = null;
        $this->handshakeHandler = null;
        $this->queryHandler = null;
        $this->prepareHandler = null;
        $this->executeHandler = null;
        $this->pingHandler = null;

        if ($this->connectPromise) {
            $this->connectPromise->reject(new \RuntimeException('Connection closed by user'));
            $this->connectPromise = null;
        }

        if ($this->currentCommand) {
            $this->currentCommand->promise->reject(new \RuntimeException('Connection closed by user'));
            $this->currentCommand = null;
        }

        while (! $this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();
            $cmd->promise->reject(new \RuntimeException('Connection closed by user'));
        }
    }

    /**
     * Get the current connection state.
     */
    public function getState(): ConnectionState
    {
        return $this->state;
    }

    /**
     * Check if the connection is ready to execute queries.
     */
    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    /**
     * Check if the connection is closed.
     */
    public function isClosed(): bool
    {
        return $this->state === ConnectionState::CLOSED;
    }

    // ================================================================================================================
    // Internal API (Used by PreparedStatement)
    // ================================================================================================================

    /**
     * @internal Called by PreparedStatement to execute bound parameters.
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
     * @internal Called by PreparedStatement to execute with streaming.
     * @return PromiseInterface<StreamStats>
     */
    /**
     * @internal Called by PreparedStatement to execute with streaming.
     * @return PromiseInterface<StreamStats>
     */
    public function executeStatementStream(
        PreparedStatement $stmt,
        array $params,
        StreamContext $context
    ): PromiseInterface {
        $executeContext = new ExecuteStreamContext($stmt, $context);

        return $this->enqueueCommand(
            CommandRequest::TYPE_EXECUTE_STREAM,
            '',
            $params,
            $stmt->id,
            $executeContext
        );
    }

    /**
     * @internal Called by PreparedStatement to close itself.
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

            case CommandRequest::TYPE_STREAM_QUERY:
                $this->state = ConnectionState::QUERYING;
                /** @var StreamContext $streamContext */
                $streamContext = $this->currentCommand->context;
                $this->queryHandler->start($this->currentCommand->sql, $this->currentCommand->promise, $streamContext);
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

            case CommandRequest::TYPE_EXECUTE_STREAM:
                $this->state = ConnectionState::EXECUTING;
                /** @var ExecuteStreamContext $ctx */
                $ctx = $this->currentCommand->context;

                $this->executeHandler->start(
                    $ctx->statement->id,
                    $this->currentCommand->params,
                    $ctx->statement->columnDefinitions,
                    $this->currentCommand->promise,
                    $ctx->streamContext
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
            $this->finishCommand(...),
            $this->finishCommand(...)
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
        $payload = \chr(0x19) . pack('V', $stmtId);
        $header = substr(pack('V', 5), 0, 3) . chr(0);
        $this->socket->write($header . $payload);
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
                        ConnectionState::QUERYING => $this->queryHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PINGING => $this->pingHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PREPARING => $this->prepareHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::EXECUTING => $this->executeHandler?->processPacket($payloadReader, $length, $seq),
                        default => null
                    };
                });

                if (! $success) {
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
        if ($this->isClosingError) {
            return;
        }

        $this->isClosingError = true;
        $this->state = ConnectionState::CLOSED;

        if ($this->connectPromise) {
            $this->connectPromise->reject($e);
            $this->connectPromise = null;
        }
        if ($this->currentCommand) {
            $this->currentCommand->promise->reject($e);
        }
        while (! $this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();
            $cmd->promise->reject(new \RuntimeException('Connection closed before execution', 0, $e));
        }
        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }
        $this->isClosingError = false;
    }

    private function handleSocketClose(): void
    {
        if ($this->isClosingError || $this->isUserClosing) {
            return;
        }

        $this->state = ConnectionState::CLOSED;

        $exception = new \RuntimeException('Connection closed unexpectedly');

        if ($this->connectPromise) {
            $this->connectPromise->reject($exception);
            $this->connectPromise = null;
        }

        if ($this->currentCommand) {
            $this->currentCommand->promise->reject($exception);
        }
    }
}
