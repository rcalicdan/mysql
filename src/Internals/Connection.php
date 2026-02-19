<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use function Hibla\async;

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
     * @var SplQueue<CommandRequest> Queue of commands waiting for execution.
     */
    private SplQueue $commandQueue;

    private ConnectionState $state = ConnectionState::DISCONNECTED;

    private ?SocketConnection $socket = null;

    private ?UncompressedPacketReader $packetReader = null;

    private ?HandshakeHandler $handshakeHandler = null;

    private ?QueryHandler $queryHandler = null;

    private ?PingHandler $pingHandler = null;

    private ?PrepareHandler $prepareHandler = null;

    private ?ExecuteHandler $executeHandler = null;

    /**
     * @var Promise<self>|null
     */
    private ?Promise $connectPromise = null;

    private ?CommandRequest $currentCommand = null;

    private readonly ConnectionParams $params;

    private bool $isClosingError = false;

    private bool $isUserClosing = false;

    /**
     * The MySQL server thread ID for this connection, received during handshake.
     * Used to send KILL QUERY on cancellation.
     */
    private int $threadId = 0;

    /**
     * Set to true when a query was cancelled mid-execution via KILL QUERY.
     *
     * The pool MUST check this via wasQueryCancelled() and run:
     *   DO SLEEP(0)
     * before returning this connection to normal use. This absorbs the stale
     * KILL flag that MySQL sets when KILL QUERY arrives after the query finishes.
     */
    private bool $wasQueryCancelled = false;

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

        /** @var Promise<self> $promise */
        $promise = new Promise();
        $this->connectPromise = $promise;
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

        return $promise;
    }

    /**
     * Pauses the connection by pausing the socket stream.
     */
    public function pause(): void
    {
        $this->socket?->pause();
    }

    /**
     * Resumes the connection by un-pausing the socket stream.
     */
    public function resume(): void
    {
        $this->socket?->resume();
    }

    /**
     * Returns the MySQL server thread ID for this connection.
     * Available after the handshake completes (i.e. after connect() resolves).
     */
    public function getThreadId(): int
    {
        return $this->threadId;
    }

    /**
     * Returns true if a query was cancelled via KILL QUERY on this connection.
     *
     * When true, the connection pool MUST issue DO SLEEP(0) before reuse to
     * absorb any stale KILL flag left by MySQL if the kill arrived after the
     * query had already finished.
     */
    public function wasQueryCancelled(): bool
    {
        return $this->wasQueryCancelled;
    }

    /**
     * Clears the cancelled flag after the pool has absorbed the stale kill.
     */
    public function clearCancelledFlag(): void
    {
        $this->wasQueryCancelled = false;
    }

    /**
     * Executes a standard SQL query (buffered).
     *
     * @return PromiseInterface<Result>
     */
    public function query(string $sql): PromiseInterface
    {
        /** @var PromiseInterface<Result> */
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);
    }


    /**
     * Streams a SELECT query row-by-row using a Generator.
     *
     * The returned promise stays PENDING until the first server event
     * (first row, completion, or error) so that cancelling the promise
     * before any data arrives correctly propagates KILL QUERY.
     *
     * After the promise resolves with a RowStream, Phase 2 cancellation
     * is available via $stream->cancel().
     *
     * @return PromiseInterface<RowStream>
     */
    public function streamQuery(string $sql, int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);

        $stream->setBackpressureHandler(function (bool $shouldPause): void {
            $shouldPause ? $this->pause() : $this->resume();
        });

        /** @var Promise<RowStream> $outerPromise */
        $outerPromise = new Promise();

        $context = new StreamContext(
            onRow: function (array $row) use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->resolve($stream);
                }
                $stream->push($row);
            },
            onComplete: function (StreamStats $stats) use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->resolve($stream);
                }
                $stream->complete($stats);
            },
            onError: function (Throwable $e) use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->reject($e);
                }
                $stream->error($e);
            }
        );

        $commandPromise = $this->enqueueCommand(
            CommandRequest::TYPE_STREAM_QUERY,
            $sql,
            context: $context
        );

        $stream->setCommandPromise($commandPromise);

        $commandPromise->then(
            $stream->markCommandFinished(...),
            $stream->error(...)
        );

        $outerPromise->onCancel(function () use ($stream): void {
            $stream->cancel();
        });

        return $outerPromise;
    }

    /**
     * Prepares a SQL statement.
     *
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        /** @var PromiseInterface<PreparedStatement> */
        return $this->enqueueCommand(CommandRequest::TYPE_PREPARE, $sql);
    }

    /**
     * Pings the server.
     *
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface
    {
        /** @var PromiseInterface<bool> */
        return $this->enqueueCommand(CommandRequest::TYPE_PING);
    }

    /**
     * Closes the connection and releases resources.
     */
    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        // Fail-safe: If closing a busy connection, ensure it kill the query on the server.
        // This prevents server-side "zombie" queries holding locks when the client disconnects abruptly.
        if (($this->state === ConnectionState::QUERYING || $this->state === ConnectionState::EXECUTING) && $this->threadId > 0) {
            $this->dispatchKillQuery($this->threadId);
        }

        $this->isUserClosing = true;
        $this->state = ConnectionState::CLOSED;

        if ($this->socket !== null) {
            $this->socket->close();
            $this->socket = null;
        }

        $this->packetReader = null;
        $this->handshakeHandler = null;
        $this->queryHandler = null;
        $this->prepareHandler = null;
        $this->executeHandler = null;
        $this->pingHandler = null;

        if ($this->connectPromise !== null) {
            $this->connectPromise->reject(
                new ConnectionException('Connection closed before establishing')
            );
            $this->connectPromise = null;
        }

        if ($this->currentCommand !== null) {
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
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<Result>
     */
    public function executeStatement(PreparedStatement $stmt, array $params): PromiseInterface
    {
        /** @var PromiseInterface<Result> */
        return $this->enqueueCommand(
            CommandRequest::TYPE_EXECUTE,
            '',
            $params,
            $stmt->id,
            $stmt
        );
    }

    /**
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<StreamStats>
     */
    public function executeStream(
        PreparedStatement $stmt,
        array $params,
        StreamContext $context
    ): PromiseInterface {
        $executeContext = new ExecuteStreamContext($stmt, $context);

        /** @var PromiseInterface<StreamStats> */
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
        /** @var PromiseInterface<void> */
        return $this->enqueueCommand(
            CommandRequest::TYPE_CLOSE_STMT,
            '',
            [],
            $stmtId
        );
    }

    /**
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<mixed>
     */
    private function enqueueCommand(
        string $type,
        string $sql = '',
        array $params = [],
        int $stmtId = 0,
        mixed $context = null
    ): PromiseInterface {
        // Prevent queuing commands on closed connections to ensure clean shutdown.
        if ($this->state === ConnectionState::CLOSED) {
            if ($type === CommandRequest::TYPE_CLOSE_STMT) {
                // Silently resolve close_stmt calls from destructors to avoid errors during shutdown.
                return Promise::resolved(null);
            }
            return Promise::rejected(new ConnectionException('Connection is closed'));
        }

        $promise = new Promise();
        $command = new CommandRequest($type, $promise, $sql, $params, $stmtId, $context);
        $this->commandQueue->enqueue($command);
        $this->processNextCommand();

        $promise->onCancel(function () use ($command): void {
            $this->handleCommandCancellation($command);
        });

        return $promise;
    }

    /**
     * Handles all edge cases when a command promise is cancelled.
     *
     * Case 1 — Command still in queue (not yet started):
     *   Just remove it. No server interaction needed.
     *
     * Case 2 — Command is currently executing:
     *   - Mark the connection as cancelled (pool must absorb stale kill flag)
     *   - Fire KILL QUERY on a separate connection (best-effort, async)
     *   - IMPORTANT: Does NOT change connection state. The state will be reset
     *     by the internal "protocol promise" when the server's ERR packet arrives.
     */
    private function handleCommandCancellation(CommandRequest $command): void
    {
        // Case 1: still in queue, hasn't started yet
        if ($this->removeFromQueue($command)) {
            return;
        }

        // Case 2: currently executing
        if ($this->currentCommand === $command) {
            $isKillable = \in_array($command->type, [
                CommandRequest::TYPE_QUERY,
                CommandRequest::TYPE_STREAM_QUERY,
                CommandRequest::TYPE_EXECUTE,
                CommandRequest::TYPE_EXECUTE_STREAM,
            ], true);

            if ($isKillable && $this->threadId > 0 && ! $this->isClosed()) {
                $this->wasQueryCancelled = true;
                $this->dispatchKillQuery($this->threadId);
            }
        }
    }

    /**
     * Attempts to remove a command from the queue.
     *
     * @return bool true if the command was found and removed, false if not in queue
     */
    private function removeFromQueue(CommandRequest $command): bool
    {
        $found = false;
        $temp = new SplQueue();

        while (! $this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();

            if ($cmd === $command) {
                $found = true;
            } else {
                $temp->enqueue($cmd);
            }
        }

        while (! $temp->isEmpty()) {
            $this->commandQueue->enqueue($temp->dequeue());
        }

        return $found;
    }

    /**
     * Opens a dedicated second connection and sends KILL QUERY <threadId>.
     * This is intentionally fire-and-forget for maximum robustness.
     */
    private function dispatchKillQuery(int $threadId): void
    {
        Connection::create($this->params)->then(
            function (Connection $killConn) use ($threadId): void {
                $killConn->query("KILL QUERY {$threadId}")
                    ->then(
                        function () use ($killConn): void {
                            $killConn->close();
                        },
                        function () use ($killConn): void {
                            $killConn->close();
                        }
                    )
                ;
            },
            function (): void {
                // Could not open kill connection. Query will run to completion.
            }
        );
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
        $this->threadId = $this->handshakeHandler?->getThreadId() ?? 0;
        $this->state = ConnectionState::READY;

        if ($this->connectPromise !== null) {
            $this->connectPromise->resolve($this);
            $this->connectPromise = null;
        }

        $this->processNextCommand();
    }

    /**
     * Dequeues and starts the next command.
     * This method decouples the user-facing promise from the internal protocol promise
     * to ensure the connection state is only reset after the server has fully responded,
     * even during cancellation.
     */
    private function processNextCommand(): void
    {
        if ($this->state !== ConnectionState::READY || $this->currentCommand !== null || $this->commandQueue->isEmpty()) {
            return;
        }

        $this->currentCommand = $this->commandQueue->dequeue();
        $command = $this->currentCommand;

        /** @var Promise<mixed> $protocolPromise */
        $protocolPromise = new Promise();

        $protocolPromise->then(
            function ($v) use ($command): void {
                $this->finishCommand();
                $command->promise->resolve($v);
            },
            function (Throwable $e) use ($command): void {
                $this->finishCommand();
                $command->promise->reject($e);
            }
        );

        switch ($command->type) {
            case CommandRequest::TYPE_QUERY:
                $this->state = ConnectionState::QUERYING;
                if ($this->queryHandler !== null) {
                    $this->queryHandler->start($command->sql, $protocolPromise);
                }

                break;

            case CommandRequest::TYPE_STREAM_QUERY:
                $this->state = ConnectionState::QUERYING;
                if ($this->queryHandler !== null) {
                    $this->queryHandler->start($command->sql, $protocolPromise, $command->context);
                }

                break;

            case CommandRequest::TYPE_PING:
                $this->state = ConnectionState::PINGING;
                if ($this->pingHandler !== null) {
                    $this->pingHandler->start($protocolPromise);
                }

                break;

            case CommandRequest::TYPE_PREPARE:
                $this->state = ConnectionState::PREPARING;
                if ($this->prepareHandler !== null) {
                    $this->prepareHandler->start($command->sql, $protocolPromise);
                }

                break;

            case CommandRequest::TYPE_EXECUTE:
                $this->state = ConnectionState::EXECUTING;
                /** @var PreparedStatement $stmt */
                $stmt = $command->context;
                if ($this->executeHandler !== null) {
                    $this->executeHandler->start($stmt->id, $command->params, $stmt->columnDefinitions, $protocolPromise);
                }

                break;

            case CommandRequest::TYPE_EXECUTE_STREAM:
                $this->state = ConnectionState::EXECUTING;
                /** @var ExecuteStreamContext $ctx */
                $ctx = $command->context;
                if ($this->executeHandler !== null) {
                    $this->executeHandler->start($ctx->statement->id, $command->params, $ctx->statement->columnDefinitions, $protocolPromise, $ctx->streamContext);
                }

                break;

            case CommandRequest::TYPE_CLOSE_STMT:
                $this->sendClosePacket($command->statementId);
                $command->promise->resolve(null);
                $this->finishCommand();

                return;
        }
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
        $header = substr(pack('V', 5), 0, 3) . \chr(0);
        $this->socket?->write($header . $payload);
    }

    private function handleData(string $chunk): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        try {
            if ($this->packetReader === null) {
                return;
            }
            $this->packetReader->append($chunk);
            while ($this->packetReader->hasPacket()) {
                $success = $this->packetReader->readPayload(function (mixed $payloadReader, mixed $length, mixed $seq): void {
                    if (! ($payloadReader instanceof \Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader) || ! \is_int($length) || ! \is_int($seq)) {
                        return;
                    }
                    match ($this->state) {
                        ConnectionState::CONNECTING => $this->handshakeHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::QUERYING => $this->queryHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PINGING => $this->pingHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::PREPARING => $this->prepareHandler?->processPacket($payloadReader, $length, $seq),
                        ConnectionState::EXECUTING => $this->executeHandler?->processPacket($payloadReader, $length, $seq),
                        default => null,
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
        $wrappedException = new ConnectionException('Failed to connect to MySQL server at ' . $this->params->host . ':' . $this->params->port . ': ' . $e->getMessage(), (int)$e->getCode(), $e);
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
        if ($this->connectPromise !== null) {
            $this->connectPromise->reject($e);
            $this->connectPromise = null;
        }
        if ($this->currentCommand !== null) {
            $this->currentCommand->promise->reject($e);
        }
        while (! $this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();
            $cmd->promise->reject(new ConnectionException('Connection closed before execution', 0, $e));
        }
        if ($this->socket !== null) {
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
        if ($this->connectPromise !== null) {
            $this->connectPromise->reject($exception);
            $this->connectPromise = null;
        }
        if ($this->currentCommand !== null) {
            $this->currentCommand->promise->reject($exception);
        }
    }

    private function wrapSocketError(Throwable $e): Throwable
    {
        $message = $e->getMessage();
        $code = $e->getCode();

        return $this->isTimeoutError($message, $code)
            ? new TimeoutException('Database connection timed out: ' . $message, $code, $e)
            : new ConnectionException('Socket error: ' . $message, $code, $e);
    }

    private function isTimeoutError(string $message, int $code): bool
    {
        if ($code === 2006) {
            return true;
        }
        foreach (['timeout', 'timed out', 'connection timeout', 'read timeout'] as $keyword) {
            if (stripos($message, $keyword) !== false) {
                return true;
            }
        }

        return false;
    }
}