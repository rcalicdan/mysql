<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Enums\ConnectionState;
use Hibla\Mysql\Handlers\ExecuteHandler;
use Hibla\Mysql\Handlers\HandshakeHandler;
use Hibla\Mysql\Handlers\PingHandler;
use Hibla\Mysql\Handlers\PrepareHandler;
use Hibla\Mysql\Handlers\QueryHandler;
use Hibla\Mysql\ValueObjects\CommandRequest;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Mysql\ValueObjects\ExecuteStreamContext;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connector;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Hibla\Socket\Interfaces\ConnectorInterface;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\TimeoutException;
use LogicException;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Packet\UncompressedPacketReader;
use SplQueue;
use Throwable;

/**
 * @internal This is a low-level, internal class. DO NOT USE IT DIRECTLY.
 *
 * Represents a single, raw TCP connection to the MySQL server. This class
 * manages the protocol state, command queue, and I/O for one socket.
 *
 * The public-facing API is provided by the `MysqlClient` class, which handles
 * connection pooling and the lifecycle of these `Connection` objects. You
 * should always interact with the database through the `MysqlClient`.
 *
 * This class is not subject to any backward compatibility (BC) guarantees. Its
 * methods, properties, and overall behavior may change without notice in any
 * patch, minor, or major version.
 *
 * @see \Hibla\Mysql\MysqlClient
 */
class Connection
{
    /**
     *  @var SplQueue<CommandRequest>
     */
    private SplQueue $commandQueue;

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

    private ?CommandRequest $currentCommand = null;

    private bool $isClosingError = false;

    private bool $isUserClosing = false;

    /**
     * @param ConnectionParams|array<string, mixed>|string $config
     * @param ConnectorInterface|null $connector
     */
    public function __construct(
        ConnectionParams|array|string $config,
        private readonly ?ConnectorInterface $connector = null
    ) {
        $this->params = match (true) {
            $config instanceof ConnectionParams => $config,
            \is_array($config) => ConnectionParams::fromArray($config),
            \is_string($config) => ConnectionParams::fromUri($config),
        };

        $this->commandQueue = new SplQueue();
    }

    /**
     * Creates and connects a new Connection instance.
     *
     * @param ConnectionParams|array<string, mixed>|string $config
     * @param ConnectorInterface|null $connector
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
     * Establishes the TCP connection and performs the MySQL Handshake.
     *
     * @return PromiseInterface<self>
     */
    public function connect(): PromiseInterface
    {
        if ($this->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(new LogicException('Connection is already active'));
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
     * Pauses the connection by pausing the socket stream.
     *
     * This removes the socket from the Event Loop's read watcher.
     * If all connections are suspended, the Event Loop will automatically exit.
     */
    public function pause(): void
    {
        if ($this->socket) {
            $this->socket->pause();
        }
    }

    /**
     * Resumes the connection by un-pausing the socket stream.
     *
     * This re-adds the socket to the Event Loop to receive data.
     */
    public function resume(): void
    {
        if ($this->socket) {
            $this->socket->resume();
        }
    }

    /**
     * Executes a standard SQL query (buffered).
     *
     * @param string $sql
     * @return PromiseInterface<Result>
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);
    }

    /**
     * Streams a SELECT query row-by-row.
     *
     * @param string $sql
     * @param callable(array): void $onRow
     * @param callable(StreamStats): void|null $onComplete
     * @param callable(Throwable): void|null $onError
     * @return PromiseInterface<StreamStats>
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
     * Prepares a SQL statement.
     *
     * @param string $sql
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PREPARE, $sql);
    }

    /**
     * Pings the server.
     *
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PING);
    }

    /**
     * Closes the connection and releases resources.
     *
     * @return void
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
            $this->connectPromise->reject(
                new ConnectionException('Connection closed before establishing')
            );
            $this->connectPromise = null;
        }

        if ($this->currentCommand) {
            $this->currentCommand->promise->reject(
                new ConnectionException('Connection closed during command execution')
            );
            $this->currentCommand = null;
        }

        while (! $this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();

            if ($cmd->type === CommandRequest::TYPE_CLOSE_STMT) {
                $cmd->promise->resolve(null);
            } else {
                $cmd->promise->reject(
                    new ConnectionException('Connection closed before command could execute')
                );
            }
        }
    }

    public function __destruct()
    {
        $this->close();
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
     * @return PromiseInterface<Result>
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
     * @return PromiseInterface<StreamStats>
     */
    public function executeStream(
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
        } catch (Throwable $e) {
            $this->handleError($e);
        }
    }

    private function handleConnectionError(Throwable $e): void
    {
        $wrappedException = new ConnectionException(
            'Failed to connect to MySQL server at ' . $this->params->host . ':' . $this->params->port . ': ' . $e->getMessage(),
            (int)$e->getCode(),
            $e
        );

        $this->handleError($wrappedException);
    }

    private function handleHandshakeError(Throwable $e): void
    {
        $this->handleError($e);
    }

    private function handleSocketError(Throwable $e): void
    {
        $wrappedException = $this->wrapSocketError($e);
        $this->handleError($wrappedException);
    }

    private function handleError(Throwable $e): void
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
            $cmd->promise->reject(
                new ConnectionException('Connection closed before execution', 0, $e)
            );
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

        $exception = new ConnectionException('Connection closed unexpectedly by the server');

        if ($this->connectPromise) {
            $this->connectPromise->reject($exception);
            $this->connectPromise = null;
        }

        if ($this->currentCommand) {
            $this->currentCommand->promise->reject($exception);
        }
    }

    private function wrapSocketError(Throwable $e): Throwable
    {
        $message = $e->getMessage();
        $code = $e->getCode();

        if ($this->isTimeoutError($message, $code)) {
            return new TimeoutException(
                'Database connection timed out: ' . $message,
                $code,
                $e
            );
        }

        return new ConnectionException(
            'Socket error: ' . $message,
            $code,
            $e
        );
    }

    private function isTimeoutError(string $message, int $code): bool
    {
        if ($code === 2006) {
            return true;
        }

        $timeoutKeywords = ['timeout', 'timed out', 'connection timeout', 'read timeout'];
        $lowerMessage = strtolower($message);

        foreach ($timeoutKeywords as $keyword) {
            if (stripos($lowerMessage, $keyword) !== false) {
                return true;
            }
        }

        return false;
    }
}
