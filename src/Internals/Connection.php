<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use function Hibla\async;
use function Hibla\await;

use Hibla\Mysql\Enums\ConnectionState;
use Hibla\Mysql\Handlers\ExecuteHandler;
use Hibla\Mysql\Handlers\HandshakeHandler;
use Hibla\Mysql\Handlers\PingHandler;
use Hibla\Mysql\Handlers\PrepareHandler;
use Hibla\Mysql\Handlers\QueryHandler;
use Hibla\Mysql\Handlers\ResetHandler;
use Hibla\Mysql\ValueObjects\CommandRequest;
use Hibla\Mysql\ValueObjects\MysqlConfig;
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
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketWriterFactory;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Packet\PacketReader;
use Rcalicdan\MySQLBinaryProtocol\Packet\PacketWriter;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;
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
     * @var SplQueue<CommandRequest>
     */
    private SplQueue $commandQueue;

    private ConnectionState $state = ConnectionState::DISCONNECTED;

    private ?SocketConnection $socket = null;

    private ?PacketReader $packetReader = null;

    private ?PacketWriter $packetWriter = null;

    private ?HandshakeHandler $handshakeHandler = null;

    private ?QueryHandler $queryHandler = null;

    private ?PingHandler $pingHandler = null;

    private ?ResetHandler $resetHandler = null;

    private ?PrepareHandler $prepareHandler = null;

    private ?ExecuteHandler $executeHandler = null;

    /**
     * @var Promise<self>|null
     */
    private ?Promise $connectPromise = null;

    private ?CommandRequest $currentCommand = null;

    private readonly MysqlConfig $params;

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
     * Tracks in-flight KILL QUERY promises keyed by thread ID.
     *
     * Keying by thread ID is intentional: MySQL has exactly one running query
     * per thread at any moment, so a second KILL for the same thread ID before
     * the first resolves is redundant. The idempotency guard in
     * dispatchKillQuery() prevents the second call from overwriting the first
     * promise and orphaning it.
     *
     * Each promise is created and registered SYNCHRONOUSLY inside
     * dispatchKillQuery() before async() schedules the fiber, so close() always
     * sees a non-empty map regardless of when it runs relative to the fiber.
     *
     * Entries are removed inside the fiber's finally block once the kill
     * promise settles, keeping the map lean across the connection lifetime.
     *
     * @var array<int, Promise<mixed>>
     */
    private array $pendingKills = [];

    /**
     * @param MysqlConfig|array<string, mixed>|string $config
     * @param ConnectorInterface|null $connector
     */
    public function __construct(
        MysqlConfig|array|string $config,
        private readonly ?ConnectorInterface $connector = null
    ) {
        $this->params = match (true) {
            $config instanceof MysqlConfig => $config,
            \is_array($config) => MysqlConfig::fromArray($config),
            \is_string($config) => MysqlConfig::fromUri($config),
        };

        /** @var SplQueue<CommandRequest> $queue */
        $queue = new SplQueue();
        $this->commandQueue = $queue;
    }

    /**
     * Creates and connects a new Connection instance.
     *
     * @param MysqlConfig|array<string, mixed>|string $config
     * @param ConnectorInterface|null $connector
     * @return PromiseInterface<self>
     */
    public static function create(
        MysqlConfig|array|string $config,
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
        $this->isUserClosing = false;

        /** @var Promise<self> $promise */
        $promise = new Promise();
        $this->connectPromise = $promise;

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
     * Writes a payload to the socket, managing chunking and packet headers.
     * Centralized implementation for all command handlers.
     *
     * @param string $payload The raw command payload.
     * @param int $sequenceId The current sequence ID, automatically incremented.
     */
    public function writePacket(string $payload, int &$sequenceId): void
    {
        if ($this->packetWriter === null || $this->socket === null) {
            throw new ConnectionException('Cannot write packet: Connection not initialized.');
        }

        $MAX_PACKET_SIZE = 16777215; // 16MB - 1 byte
        $length = \strlen($payload);
        $offset = 0;

        // If payload is larger than 16MB, split it
        while ($length >= $MAX_PACKET_SIZE) {
            $chunk = substr($payload, $offset, $MAX_PACKET_SIZE);
            $packet = $this->packetWriter->write($chunk, $sequenceId);

            $this->socket->write($packet);

            $sequenceId++;
            $length -= $MAX_PACKET_SIZE;
            $offset += $MAX_PACKET_SIZE;
        }

        $chunk = substr($payload, $offset);
        $packet = $this->packetWriter->write($chunk, $sequenceId);

        $this->socket->write($packet);
        $sequenceId++;
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

        /** @var PromiseInterface<mixed> $commandPromise */
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
     * Resets the connection state via COM_RESET_CONNECTION.
     *
     * @return PromiseInterface<bool>
     */
    public function reset(): PromiseInterface
    {
        /** @var PromiseInterface<bool> */
        return $this->enqueueCommand(CommandRequest::TYPE_RESET);
    }

    /**
     * Closes the connection and releases resources.
     *
     * If KILL QUERY packets are in-flight (dispatched either by a prior
     * cancellation or by the busy-connection guard below), teardown is
     * deferred until every pending kill settles. This closes the race where
     * the socket would otherwise be destroyed while the kill fiber is still
     * mid-connect, leaving the server-side query holding row/table locks with
     * no client left to receive the eventual ERR packet.
     *
     * The snapshot of $pendingKills taken here is stable: state is set to
     * CLOSED before the snapshot, so no new kills can be dispatched after
     * this point — handleCommandCancellation() guards with !isClosed(), and
     * the busy-connection dispatch above only fires before the state flag is
     * set. The two code paths are therefore mutually exclusive at snapshot time.
     *
     * Teardown is always guaranteed to run: each pending kill is wrapped in a
     * KILL_TIMEOUT_SECONDS hard timeout, so a slow or unreachable kill
     * side-channel can never block shutdown indefinitely.
     */
    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        // Fail-safe: if closing a busy connection, ensure the query is killed
        // on the server. This prevents server-side "zombie" queries holding
        // locks when the client disconnects abruptly.
        //
        // dispatchKillQuery() is a no-op if a kill is already in-flight for
        // this thread ID (e.g. from a prior cancellation), so there is no
        // double-send and no risk of overwriting an existing pendingKills entry.
        if (
            ($this->state === ConnectionState::QUERYING || $this->state === ConnectionState::EXECUTING)
            && $this->threadId > 0
            && $this->params->enableServerSideCancellation
        ) {
            $this->dispatchKillQuery($this->threadId);
        }

        $this->isUserClosing = true;
        $this->state = ConnectionState::CLOSED;

        // Snapshot pendingKills AFTER state is set to CLOSED and AFTER the
        // conditional dispatch above. At this point no new kills can be added:
        //   - handleCommandCancellation() guards with !isClosed()
        //   - close() itself only dispatches in the block above
        // The snapshot is therefore complete and stable.
        //
        // Both branches of the then() call teardown() so that a rejected or
        // timed-out kill never silently skips cleanup.
        if ($this->pendingKills !== []) {
            $this->awaitPendingKills()->then(
                $this->teardown(...),
                $this->teardown(...)
            );

            return;
        }

        $this->teardown();
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
     * @param array<int, mixed> $params
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
     * @param array<int, mixed> $params
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
     * Enqueues a command for execution and returns a promise for its result.
     *
     * @param array<int, mixed> $params
     * @return PromiseInterface<mixed>
     */
    private function enqueueCommand(
        string $type,
        string $sql = '',
        array $params = [],
        int $stmtId = 0,
        mixed $context = null
    ): PromiseInterface {
        if ($this->state === ConnectionState::CLOSED) {
            if ($type === CommandRequest::TYPE_CLOSE_STMT) {
                return Promise::resolved(null);
            }

            return Promise::rejected(new ConnectionException('Connection is closed'));
        }

        /** @var Promise<mixed> $promise */
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
        if ($this->removeFromQueue($command)) {
            return;
        }

        if ($this->currentCommand === $command) {
            $isKillable = \in_array($command->type, [
                CommandRequest::TYPE_QUERY,
                CommandRequest::TYPE_STREAM_QUERY,
                CommandRequest::TYPE_EXECUTE,
                CommandRequest::TYPE_EXECUTE_STREAM,
            ], true);

            // Respect the per-connection cancellation policy set by the pool.
            // When disabled, promise cancellation only changes the promise state —
            // the server-side query runs to completion and the connection is
            // returned to the pool normally without a stale kill flag to absorb.
            if ($isKillable && $this->params->enableServerSideCancellation && $this->threadId > 0 && ! $this->isClosed()) {
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
        /** @var SplQueue<CommandRequest> $temp */
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
            $cmd = $temp->dequeue();
            $this->commandQueue->enqueue($cmd);
        }

        return $found;
    }

    /**
     * Opens a dedicated side-channel connection and sends KILL QUERY <threadId>.
     *
     * Key design properties:
     *
     *   1. IDEMPOTENT — if a kill is already in-flight for this thread ID the
     *      method is a no-op. This prevents the double-dispatch race where both
     *      handleCommandCancellation() and close() independently detect a live
     *      query and call dispatchKillQuery() for the same thread, with the
     *      second call overwriting pendingKills[$threadId] and orphaning the
     *      first promise.
     *
     *   2. SYNCHRONOUS REGISTRATION — $killPromise is created and stored in
     *      $pendingKills BEFORE async() schedules the fiber. This closes the
     *      tick-boundary race where close() runs in the same tick as
     *      dispatchKillQuery() but before the fiber has executed, making
     *      pendingKills appear empty to close().
     *
     *   3. BOUNDED — the kill work is wrapped in KILL_TIMEOUT_SECONDS so a
     *      slow or unreachable side-channel never blocks teardown forever.
     *
     *   4. SAFE AFTER TEARDOWN — the fiber captures only $this->params and
     *      $this->connector, both of which are readonly and never nulled by
     *      teardown(). The fiber therefore remains safe to execute even if
     *      teardown() has already run on the parent connection.
     */
    private function dispatchKillQuery(int $threadId): void
    {
        // Idempotency guard — a kill is already in-flight for this thread.
        // Sending a second one is redundant and would orphan the first promise,
        // causing awaitPendingKills() to never see it resolve.
        if (isset($this->pendingKills[$threadId])) {
            return;
        }

        // Registered synchronously so close() always sees a non-empty
        // pendingKills map regardless of when it runs relative to the fiber.
        /** @var Promise<mixed> $killPromise */
        $killPromise = new Promise();
        $this->pendingKills[$threadId] = $killPromise;

        // Executes in a detached fiber (via async) to:
        //   1. Prevent blocking the current call stack (especially destructors
        //      or close).
        //   2. Root the Promise chain in the Event Loop so PHP's GC doesn't
        //      destroy it prematurely when the parent method returns.
        try {
            async(function () use ($threadId, $killPromise): void {
                try {
                    $timedKill = Promise::timeout(
                        Promise::resolved(null)->then(function () use ($threadId) {
                            return Connection::create($this->params, $this->connector)
                                ->then(function ($killConn) use ($threadId) {
                                    return $killConn->query("KILL QUERY {$threadId}")
                                        ->finally(function () use ($killConn) {
                                            $killConn->close();
                                        })
                                    ;
                                })
                            ;
                        }),
                        $this->params->killTimeoutSeconds
                    );

                    await($timedKill);
                    $killPromise->resolve(null);
                } catch (Throwable) {
                    $killPromise->resolve(null);
                } finally {
                    unset($this->pendingKills[$threadId]);
                }
            });
        } catch (Throwable $e) {
            // If async() throws synchronously (e.g., event loop is dead during PHP shutdown),
            // safely discard the kill attempt so it don't crash the destructor.
            unset($this->pendingKills[$threadId]);
            $killPromise->resolve(null);
        }
    }

    /**
     * Returns a promise that resolves once every in-flight KILL QUERY promise
     * has reached a terminal state (fulfilled, rejected, or timed out).
     *
     * Uses Promise::allSettled() rather than Promise::all() so that a failed
     * or timed-out kill never short-circuits teardown — we always want every
     * kill to reach a terminal state before we proceed.
     *
     * The snapshot of $pendingKills taken here is stable: by the time this
     * method is called from close(), state is already CLOSED and no new kills
     * can be dispatched (handleCommandCancellation guards with !isClosed(), and
     * close() only dispatches before setting the state flag).
     *
     * @return PromiseInterface<void>
     */
    private function awaitPendingKills(): PromiseInterface
    {
        if ($this->pendingKills === []) {
            // @phpstan-ignore-next-line
            return Promise::resolved(null);
        }

        return Promise::allSettled($this->pendingKills)
            ->then(function () {
                $this->pendingKills = [];
            })
        ;
    }

    /**
     * Performs the actual resource release after all pending kills have settled.
     *
     * Extracted from close() so it can be invoked either immediately (when no
     * kills are in-flight) or deferred (after awaitPendingKills() resolves).
     * Keeping teardown separate ensures the two code paths stay in sync and
     * cannot diverge over time.
     *
     * Note: this method intentionally does NOT null $this->params or
     * $this->connector. Those are readonly and may still be referenced by
     * kill fibers that are running concurrently with or after teardown.
     */
    private function teardown(): void
    {
        if ($this->socket !== null) {
            $this->socket->close();
            $this->socket = null;
        }

        $this->packetReader = null;
        $this->packetWriter = null;
        $this->handshakeHandler = null;
        $this->queryHandler = null;
        $this->prepareHandler = null;
        $this->executeHandler = null;
        $this->pingHandler = null;
        $this->resetHandler = null;

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

    /**
     * Wires a typed protocol promise to a command's user-facing promise,
     * ensuring finishCommand() is called on both success and failure.
     *
     * @param Promise<mixed> $protocolPromise
     */
    private function wireProtocolPromise(Promise $protocolPromise, CommandRequest $command): void
    {
        $protocolPromise->then(
            function (mixed $value) use ($command): void {
                $this->finishCommand();
                $command->promise->resolve($value);
            },
            function (Throwable $e) use ($command): void {
                $this->finishCommand();
                $command->promise->reject($e);
            }
        );
    }

    /**
     * Dequeues and starts the next command.
     *
     * Decouples the user-facing promise from the internal protocol promise
     * to ensure the connection state is only reset after the server has fully
     * responded, even during cancellation.
     */
    private function processNextCommand(): void
    {
        if (
            $this->state !== ConnectionState::READY
            || $this->currentCommand !== null
            || $this->commandQueue->isEmpty()
        ) {
            return;
        }

        $command = $this->commandQueue->dequeue();
        $this->currentCommand = $command;

        switch ($command->type) {
            case CommandRequest::TYPE_QUERY:
                $this->state = ConnectionState::QUERYING;
                $queryHandler = $this->queryHandler;
                if ($queryHandler !== null) {
                    /** @var Promise<Result|StreamStats> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    $queryHandler->start($command->sql, $protocolPromise);
                }

                break;

            case CommandRequest::TYPE_STREAM_QUERY:
                $this->state = ConnectionState::QUERYING;
                $queryHandler = $this->queryHandler;
                if ($queryHandler !== null) {
                    /** @var Promise<Result|StreamStats> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    /** @var StreamContext|null $streamContext */
                    $streamContext = $command->context;
                    $queryHandler->start($command->sql, $protocolPromise, $streamContext);
                }

                break;

            case CommandRequest::TYPE_PING:
                $this->state = ConnectionState::PINGING;
                $pingHandler = $this->pingHandler;
                if ($pingHandler !== null) {
                    /** @var Promise<bool> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    $pingHandler->start($protocolPromise);
                }

                break;

            case CommandRequest::TYPE_RESET:
                $this->state = ConnectionState::RESETTING;
                $resetHandler = $this->resetHandler;
                if ($resetHandler !== null) {
                    /** @var Promise<bool> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    $resetHandler->start($protocolPromise);
                }

                break;

            case CommandRequest::TYPE_PREPARE:
                $this->state = ConnectionState::PREPARING;
                $prepareHandler = $this->prepareHandler;
                if ($prepareHandler !== null) {
                    /** @var Promise<PreparedStatement> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    $prepareHandler->start($command->sql, $protocolPromise);
                }

                break;

            case CommandRequest::TYPE_EXECUTE:
                $this->state = ConnectionState::EXECUTING;
                /** @var PreparedStatement $stmt */
                $stmt = $command->context;
                $executeHandler = $this->executeHandler;
                if ($executeHandler !== null) {
                    /** @var Promise<Result|StreamStats> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    /** @var array<int, mixed> $params */
                    $params = $command->params;
                    $executeHandler->start($stmt->id, $params, $stmt->columnDefinitions, $protocolPromise);
                }

                break;

            case CommandRequest::TYPE_EXECUTE_STREAM:
                $this->state = ConnectionState::EXECUTING;
                /** @var ExecuteStreamContext $ctx */
                $ctx = $command->context;
                $executeHandler = $this->executeHandler;
                if ($executeHandler !== null) {
                    /** @var Promise<Result|StreamStats> $protocolPromise */
                    $protocolPromise = new Promise();
                    $this->wireProtocolPromise($protocolPromise, $command);
                    /** @var array<int, mixed> $params */
                    $params = $command->params;
                    $executeHandler->start(
                        $ctx->statement->id,
                        $params,
                        $ctx->statement->columnDefinitions,
                        $protocolPromise,
                        $ctx->streamContext
                    );
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
        $seq = 0;
        $this->writePacket($payload, $seq);
    }

    private function handleSocketConnected(SocketConnection $socket): void
    {
        if ($this->state !== ConnectionState::CONNECTING) {
            $socket->close();

            return;
        }

        $this->socket = $socket;

        // Initialize with uncompressed readers and writers
        $readerFactory = new DefaultPacketReaderFactory();
        $this->packetReader = $readerFactory->createWithDefaultSettings();

        $writerFactory = new DefaultPacketWriterFactory();
        $this->packetWriter = $writerFactory->createWithDefaultSettings();

        $commandBuilder = new CommandBuilder();
        $this->handshakeHandler = new HandshakeHandler($socket, $this->params);
        $this->queryHandler = new QueryHandler($this, $commandBuilder);
        $this->pingHandler = new PingHandler($this);
        $this->resetHandler = new ResetHandler($this);
        $this->prepareHandler = new PrepareHandler($this, $commandBuilder);
        $this->executeHandler = new ExecuteHandler($this, $commandBuilder);

        $this->socket->on('data', $this->handleData(...));
        $this->socket->on('close', $this->handleSocketClose(...));
        $this->socket->on('error', $this->handleSocketError(...));

        /** @var UncompressedPacketReader $reader */
        $reader = $this->packetReader;

        $this->handshakeHandler->start($reader)->then(
            $this->handleHandshakeSuccess(...),
            $this->handleHandshakeError(...)
        );
    }

    private function handleHandshakeSuccess(int $nextSeqId): void
    {
        $this->threadId = $this->handshakeHandler?->getThreadId() ?? 0;
        $this->state = ConnectionState::READY;

        // Upgrade protocol if compression was negotiated
        if ($this->handshakeHandler !== null && $this->handshakeHandler->isCompressionEnabled()) {
            $readerFactory = new DefaultPacketReaderFactory();
            $this->packetReader = $readerFactory->createCompressed();

            $writerFactory = new DefaultPacketWriterFactory();
            $this->packetWriter = $writerFactory->createCompressed();
        }

        if ($this->connectPromise !== null) {
            $this->connectPromise->resolve($this);
            $this->connectPromise = null;
        }

        $this->processNextCommand();
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
                $success = $this->packetReader->readPayload(
                    function (mixed $payloadReader, mixed $length, mixed $seq): void {
                        if (
                            ! ($payloadReader instanceof PayloadReader)
                            || ! \is_int($length)
                            || ! \is_int($seq)
                        ) {
                            return;
                        }

                        match ($this->state) {
                            ConnectionState::CONNECTING => $this->handshakeHandler?->processPacket($payloadReader, $length, $seq),
                            ConnectionState::QUERYING => $this->queryHandler?->processPacket($payloadReader, $length, $seq),
                            ConnectionState::PINGING => $this->pingHandler?->processPacket($payloadReader, $length, $seq),
                            ConnectionState::RESETTING => $this->resetHandler?->processPacket($payloadReader, $length, $seq),
                            ConnectionState::PREPARING => $this->prepareHandler?->processPacket($payloadReader, $length, $seq),
                            ConnectionState::EXECUTING => $this->executeHandler?->processPacket($payloadReader, $length, $seq),
                            default => null,
                        };
                    }
                );

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
        $wrapped = new ConnectionException(
            'Failed to connect to MySQL server at '
                . $this->params->host . ':' . $this->params->port
                . ': ' . $e->getMessage(),
            (int) $e->getCode(),
            $e
        );

        $this->handleError($wrapped);
    }

    private function handleHandshakeError(Throwable $e): void
    {
        $this->handleError($e);
    }

    private function handleSocketError(Throwable $e): void
    {
        $this->handleError($this->wrapSocketError($e));
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
