<?php

declare(strict_types=1);

namespace Hibla\Mysql\Manager;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Exceptions\PoolException;
use Hibla\Mysql\Internals\Connection as MysqlConnection;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Promise\Exceptions\TimeoutException;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectorInterface;
use InvalidArgumentException;
use SplQueue;
use Throwable;

/**
 * @internal This is a low-level, internal class. DO NOT USE IT DIRECTLY.
 *
 * Manages a pool of asynchronous MySQL connections. This class is the core
 * component responsible for creating, reusing, and managing the lifecycle
 * of individual `Connection` objects to prevent resource exhaustion.
 *
 * All pooling logic is handled automatically by the `MysqlClient`. You should
 * never need to interact with the `PoolManager` directly.
 *
 * ## Cancellation and Connection Reuse
 *
 * When a query is cancelled via KILL QUERY, MySQL sets a stale kill flag on
 * the server-side thread. Before this connection can be safely reused, the
 * pool must absorb this flag by issuing `DO SLEEP(0)` asynchronously. During
 * this absorption phase the connection is tracked in `$drainingConnections`
 * to guarantee it is never lost — even if `close()` is called mid-drain.
 *
 * ## Connection Reset
 *
 * If `resetConnection` is enabled, the pool will issue `COM_RESET_CONNECTION`
 * asynchronously before returning a connection to the idle pool. This clears
 * session state (variables, temporary tables, transactions) to prevent state
 * leakage between requests. This phase is also tracked via `$drainingConnections`.
 *
 * ## Query Cancellation Toggle
 *
 * The `$enableServerSideCancellation` constructor parameter controls whether
 * cancelling a query promise causes a KILL QUERY to be dispatched to the
 * server. When disabled, cancellation only transitions the promise state —
 * the server-side query runs to completion and the connection is eventually
 * returned to the pool normally.
 *
 * ## Kill Connections
 *
 * KILL QUERY requires a separate TCP connection to the same server. These
 * kill connections are intentionally created outside the pool — they are brief,
 * critical, and must never be blocked by pool capacity limits.
 *
 * This class is not subject to any backward compatibility (BC) guarantees.
 */
class PoolManager
{
    /**
     * @var SplQueue<MysqlConnection> Idle connections available for reuse.
     */
    private SplQueue $pool;

    /**
     * @var SplQueue<Promise<MysqlConnection>> Callers waiting for a connection.
     */
    private SplQueue $waiters;

    private int $maxSize;

    private int $activeConnections = 0;

    private ConnectionParams $connectionParams;

    private ?ConnectorInterface $connector;

    private bool $configValidated = false;

    private int $idleTimeoutNanos;

    private int $maxLifetimeNanos;

    private int $maxWaiters;

    private float $acquireTimeout;

    private PoolException $exhaustedException;

    /**
     * Real-time count of pending waiters. Decremented via finally() hook on
     * the waiter promise, so it reflects the true number of requests still
     * waiting regardless of how the promise settles (resolve, reject, cancel).
     */
    private int $pendingWaiters = 0;

    /**
     * @var array<int, int> Last-used timestamp (nanoseconds) keyed by spl_object_id.
     */
    private array $connectionLastUsed = [];

    /**
     * @var array<int, int> Creation timestamp (nanoseconds) keyed by spl_object_id.
     */
    private array $connectionCreatedAt = [];

    /**
     * Connections currently absorbing a stale KILL flag via DO SLEEP(0) or resetting
     * via COM_RESET_CONNECTION. Tracked to prevent leaks if close() is called during drain.
     *
     * @var array<int, MysqlConnection> keyed by spl_object_id.
     */
    private array $drainingConnections = [];

    /**
     * Connections currently checked out by the client.
     * Tracked to ensure they are closed if the pool is shut down while requests are active.
     *
     * @var array<int, MysqlConnection> keyed by spl_object_id.
     */
    private array $activeConnectionsMap = [];

    private bool $isClosing = false;

    /**
     * @param ConnectionParams|array<string, mixed>|string $config
     * @param int $maxSize Maximum number of connections in the pool.
     * @param int $idleTimeout Seconds before an idle connection is closed.
     * @param int $maxLifetime Seconds before a connection is rotated.
     * @param bool $enableServerSideCancellation Whether cancelling a query promise dispatches
     *                                           KILL QUERY to the server. Defaults to true.
     * @param int $maxWaiters Maximum number of requests allowed in the queue
     *                        waiting for a connection. 0 means unlimited.
     * @param float $acquireTimeout Maximum seconds to wait for a connection before giving up.
     *                              0.0 means unlimited (wait forever).
     * @param ConnectorInterface|null $connector Optional custom socket connector.
     */
    public function __construct(
        ConnectionParams|array|string $config,
        int $maxSize = 10,
        int $idleTimeout = 300,
        int $maxLifetime = 3600,
        bool $enableServerSideCancellation = false,
        int $maxWaiters = 0,
        float $acquireTimeout = 0.0,
        ?ConnectorInterface $connector = null,
    ) {
        $params = match (true) {
            $config instanceof ConnectionParams => $config,
            \is_array($config) => ConnectionParams::fromArray($config),
            \is_string($config) => ConnectionParams::fromUri($config),
        };

        // Apply the pool-level override if it differs from what ConnectionParams
        // carries. withQueryCancellation() returns a new immutable instance so
        // the caller's original ConnectionParams is never mutated.
        $this->connectionParams = $params->enableServerSideCancellation === $enableServerSideCancellation
            ? $params
            : $params->withQueryCancellation($enableServerSideCancellation);

        if ($maxSize <= 0) {
            throw new InvalidArgumentException('Pool max size must be greater than 0');
        }

        if ($idleTimeout <= 0) {
            throw new InvalidArgumentException('Idle timeout must be greater than 0');
        }

        if ($maxLifetime <= 0) {
            throw new InvalidArgumentException('Max lifetime must be greater than 0');
        }

        if ($maxWaiters < 0) {
            throw new InvalidArgumentException('Max waiters must be 0 or greater');
        }

        if ($acquireTimeout < 0.0) {
            throw new InvalidArgumentException('Acquire timeout must be 0.0 or greater');
        }

        // Optimization: Pre-instantiate the exception to avoid stack trace allocation
        // during high-load rejection scenarios.
        $this->exhaustedException = new PoolException(
            "Connection pool exhausted. Max waiters limit ({$maxWaiters}) reached."
        );

        $this->configValidated = true;
        $this->maxWaiters = $maxWaiters;
        $this->acquireTimeout = $acquireTimeout;
        $this->maxSize = $maxSize;
        $this->connector = $connector;
        $this->idleTimeoutNanos = $idleTimeout * 1_000_000_000;
        $this->maxLifetimeNanos = $maxLifetime * 1_000_000_000;
        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
    }

    /**
     * Asynchronously acquires a connection from the pool.
     *
     * Uses "Check-on-Borrow" strategy:
     * 1. Idle timeout exceeded → discard.
     * 2. Max lifetime exceeded → discard.
     * 3. Not ready / closed → discard.
     *
     * If no idle connection is available and the pool is not at capacity,
     * a new connection is created. Otherwise the caller is queued as a waiter.
     *
     * Waiter promises support cancellation and timeouts:
     * - If cancelled before connection acquisition, it is skipped.
     * - If acquireTimeout is set and exceeded, the promise rejects with TimeoutException.
     *
     * @return PromiseInterface<MysqlConnection>
     */
    public function get(): PromiseInterface
    {
        while (! $this->pool->isEmpty()) {
            /** @var MysqlConnection $connection */
            $connection = $this->pool->dequeue();

            $connId = spl_object_id($connection);
            $now = (int) hrtime(true);
            $lastUsed = $this->connectionLastUsed[$connId] ?? 0;
            $createdAt = $this->connectionCreatedAt[$connId] ?? 0;

            if (($now - $lastUsed) > $this->idleTimeoutNanos) {
                $this->removeConnection($connection);

                continue;
            }

            if (($now - $createdAt) > $this->maxLifetimeNanos) {
                $this->removeConnection($connection);

                continue;
            }

            if (! $connection->isReady() || $connection->isClosed()) {
                $this->removeConnection($connection);

                continue;
            }

            unset($this->connectionLastUsed[$connId]);

            // Mark as active so it is tracked if closed mid-usage.
            $this->activeConnectionsMap[$connId] = $connection;

            $connection->resume();

            return Promise::resolved($connection);
        }

        if ($this->activeConnections < $this->maxSize) {
            return $this->createNewConnection();
        }

        if ($this->maxWaiters > 0 && $this->pendingWaiters >= $this->maxWaiters) {
            return Promise::rejected($this->exhaustedException);
        }

        // At capacity — enqueue a waiter.
        /** @var Promise<MysqlConnection> $waiterPromise */
        $waiterPromise = new Promise();

        // If an acquire timeout is configured, schedule a timer to fail the request
        // if a connection is not obtained in time.
        if ($this->acquireTimeout > 0.0) {
            $timerId = Loop::addTimer($this->acquireTimeout, function () use ($waiterPromise): void {
                if ($waiterPromise->isPending()) {
                    $waiterPromise->reject(new TimeoutException(
                        $this->acquireTimeout
                    ));
                }
            });

            // Decrement the real-time counter and cancel the timer regardless of
            // outcome (success, failure, or user cancellation).
            $waiterPromise->finally(function () use ($timerId): void {
                $this->pendingWaiters--;
                Loop::cancelTimer($timerId);
            })->catch(static function (): void {
                // Suppress exception propagation on the cleanup chain.
            });
        } else {
            // No timeout — just decrement the counter when the waiter settles.
            $waiterPromise->finally(function (): void {
                $this->pendingWaiters--;
            })->catch(static function (): void {
                // Suppress exception propagation on the cleanup chain.
            });
        }

        $this->waiters->enqueue($waiterPromise);
        $this->pendingWaiters++;

        return $waiterPromise;
    }
    /**
     * Releases a connection back to the pool.
     *
     * Determines whether the connection needs to absorb a stale kill flag,
     * undergo a COM_RESET_CONNECTION flush, or if it can be parked cleanly.
     *
     * @param MysqlConnection $connection
     */
    public function release(MysqlConnection $connection): void
    {
        if ($connection->isClosed()) {
            $this->removeConnection($connection);
            $this->satisfyNextWaiter();

            return;
        }

        // 1. Absorb stale kill flag before the connection can be reused or reset.
        if ($connection->wasQueryCancelled()) {
            $this->drainAndRelease($connection);

            return;
        }

        // If the connection is not in a READY state (e.g., still QUERYING) and
        // was not explicitly cancelled, it means it was released in a dirty or
        // corrupted state. It cannot safely park in the idle pool.
        if (! $connection->isReady()) {
            $this->removeConnection($connection);
            $this->satisfyNextWaiter();

            return;
        }

        // 2. Perform connection state reset if enabled.
        if ($this->connectionParams->resetConnection) {
            $this->resetAndRelease($connection);

            return;
        }

        $this->releaseClean($connection);
    }

    /**
     * Retrieves statistics about the current state of the connection pool.
     *
     * @return array<string, mixed> An associative array with pool metrics.
     */
    public function getStats(): array
    {
        return [
            'active_connections' => $this->activeConnections,
            'pooled_connections' => $this->pool->count(),
            'waiting_requests' => $this->pendingWaiters,
            'draining_connections' => \count($this->drainingConnections),
            'max_size' => $this->maxSize,
            'max_waiters' => $this->maxWaiters,
            'acquire_timeout' => $this->acquireTimeout,
            'config_validated' => $this->configValidated,
            'tracked_connections' => \count($this->connectionCreatedAt),
            'query_cancellation_enabled' => $this->connectionParams->enableServerSideCancellation,
            'compression_enabled' => $this->connectionParams->compress,
            'reset_connection_enabled' => $this->connectionParams->resetConnection,
            'multi_statements_enabled' => $this->connectionParams->multiStatements,
        ];
    }

    /**
     * Closes all connections in all states (idle, draining, active) and rejects all waiters.
     */
    /**
     * Closes all connections in all states (idle, draining, active) and rejects all waiters.
     */
    public function close(): void
    {
        $this->isClosing = true;

        while (! $this->pool->isEmpty()) {
            $connection = $this->pool->dequeue();
            if (! $connection->isClosed()) {
                $connection->close();
            }
        }

        // Close connections that are mid-drain so they are not leaked.
        foreach ($this->drainingConnections as $connection) {
            if (! $connection->isClosed()) {
                $connection->close();
            }
        }
        $this->drainingConnections = [];

        // Close active connections to prevent hanging the event loop.
        foreach ($this->activeConnectionsMap as $connection) {
            if (! $connection->isClosed()) {
                $connection->close();
            }
        }
        $this->activeConnectionsMap = [];

        while (! $this->waiters->isEmpty()) {
            /** @var Promise<MysqlConnection> $promise */
            $promise = $this->waiters->dequeue();
            if (! $promise->isCancelled()) {
                $promise->reject(new PoolException('Pool is being closed'));
            }
        }

        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
        $this->activeConnections = 0;
        $this->pendingWaiters = 0;
        $this->connectionLastUsed = [];
        $this->connectionCreatedAt = [];
        $this->isClosing = false;
    }

    /**
     * Pings all idle connections to verify health.
     *
     * @return PromiseInterface<array<string, int>>
     */
    public function healthCheck(): PromiseInterface
    {
        /** @var Promise<array<string, int>> $promise */
        $promise = new Promise();

        $stats = [
            'total_checked' => 0,
            'healthy' => 0,
            'unhealthy' => 0,
        ];

        /** @var SplQueue<MysqlConnection> $tempQueue */
        $tempQueue = new SplQueue();

        /** @var array<int, PromiseInterface<bool>> $checkPromises */
        $checkPromises = [];

        while (! $this->pool->isEmpty()) {
            /** @var MysqlConnection $connection */
            $connection = $this->pool->dequeue();
            $stats['total_checked']++;
            $connection->resume();

            $checkPromises[] = $connection->ping()
                ->then(
                    function () use ($connection, $tempQueue, &$stats): void {
                        $stats['healthy']++;
                        $connection->pause();
                        $connId = spl_object_id($connection);
                        $this->connectionLastUsed[$connId] = (int) hrtime(true);
                        $tempQueue->enqueue($connection);
                    },
                    function () use ($connection, &$stats): void {
                        $stats['unhealthy']++;
                        $this->removeConnection($connection);
                    }
                );
        }

        Promise::all($checkPromises)
            ->then(
                function () use ($promise, $tempQueue, &$stats): void {
                    while (! $tempQueue->isEmpty()) {
                        $this->pool->enqueue($tempQueue->dequeue());
                    }
                    $promise->resolve($stats);
                },
                function (Throwable $e) use ($promise, $tempQueue): void {
                    while (! $tempQueue->isEmpty()) {
                        $this->pool->enqueue($tempQueue->dequeue());
                    }
                    $promise->reject($e);
                }
            )
        ;

        return $promise;
    }

    /**
     * Absorbs a stale KILL flag by issuing `DO SLEEP(0)` on the connection.
     *
     * The connection is tracked in `$drainingConnections` for the duration so
     * it cannot be lost if `close()` is called while the drain is in progress.
     */
    private function drainAndRelease(MysqlConnection $connection): void
    {
        $connId = spl_object_id($connection);

        unset($this->activeConnectionsMap[$connId]);

        if ($this->isClosing) {
            $this->removeConnection($connection);

            return;
        }

        $this->drainingConnections[$connId] = $connection;

        $connection->query('DO SLEEP(0)')
            ->then(
                function () use ($connection, $connId): void {
                    unset($this->drainingConnections[$connId]);

                    if ($this->isClosing) {
                        $this->removeConnection($connection);

                        return;
                    }

                    $connection->clearCancelledFlag();

                    // Mark active again for releaseClean or reset logic
                    $this->activeConnectionsMap[$connId] = $connection;

                    if ($this->connectionParams->resetConnection) {
                        $this->resetAndRelease($connection);
                    } else {
                        $this->releaseClean($connection);
                    }
                },
                function () use ($connection, $connId): void {
                    // ERR 1317 "Query execution was interrupted" — expected,
                    // means the kill arrived after query completion. Flag consumed.
                    unset($this->drainingConnections[$connId]);

                    if ($this->isClosing) {
                        $this->removeConnection($connection);

                        return;
                    }

                    $connection->clearCancelledFlag();

                    // Connection may no longer be ready after the error packet.
                    if ($connection->isClosed() || ! $connection->isReady()) {
                        $this->removeConnection($connection);
                        $this->satisfyNextWaiter();

                        return;
                    }

                    $this->activeConnectionsMap[$connId] = $connection;

                    if ($this->connectionParams->resetConnection) {
                        $this->resetAndRelease($connection);
                    } else {
                        $this->releaseClean($connection);
                    }
                }
            )
        ;
    }

    /**
     * Issues a COM_RESET_CONNECTION to clear session state before making
     * it available for the next caller. Tracked in drainingConnections.
     */
    private function resetAndRelease(MysqlConnection $connection): void
    {
        $connId = spl_object_id($connection);

        unset($this->activeConnectionsMap[$connId]);

        if ($this->isClosing) {
            $this->removeConnection($connection);

            return;
        }

        $this->drainingConnections[$connId] = $connection;

        $connection->reset()->then(
            function () use ($connection, $connId): void {
                unset($this->drainingConnections[$connId]);

                if ($this->isClosing) {
                    $this->removeConnection($connection);

                    return;
                }

                $this->activeConnectionsMap[$connId] = $connection;
                $this->releaseClean($connection);
            },
            function () use ($connection, $connId): void {
                unset($this->drainingConnections[$connId]);

                // If reset fails, the connection state is tainted. Drop it entirely.
                $this->removeConnection($connection);
                $this->satisfyNextWaiter();
            }
        );
    }

    /**
     * Releases a clean connection: either hands it to a waiting caller
     * or parks it in the idle pool.
     */
    private function releaseClean(MysqlConnection $connection): void
    {
        // Hand directly to a non-cancelled waiter if one exists.
        $waiter = $this->dequeueActiveWaiter();

        if ($waiter !== null) {
            $connection->resume();
            // Connection remains in activeConnectionsMap.
            $waiter->resolve($connection);

            return;
        }

        // No waiters — park in idle pool.
        $connection->pause();

        $connId = spl_object_id($connection);
        $now = (int) hrtime(true);
        $createdAt = $this->connectionCreatedAt[$connId] ?? 0;

        if (($now - $createdAt) > $this->maxLifetimeNanos) {
            $this->removeConnection($connection);

            return;
        }

        $this->connectionLastUsed[$connId] = $now;

        // Remove from active, move to idle pool.
        unset($this->activeConnectionsMap[$connId]);
        $this->pool->enqueue($connection);
    }

    /**
     * Creates a new connection and resolves the returned promise on success.
     *
     * @return Promise<MysqlConnection>
     */
    private function createNewConnection(): Promise
    {
        $this->activeConnections++;

        /** @var Promise<MysqlConnection> $promise */
        $promise = new Promise();

        MysqlConnection::create($this->connectionParams, $this->connector)
            ->then(
                function (MysqlConnection $connection) use ($promise): void {
                    $connId = spl_object_id($connection);
                    $this->connectionCreatedAt[$connId] = (int) hrtime(true);

                    // Mark as active.
                    $this->activeConnectionsMap[$connId] = $connection;

                    $promise->resolve($connection);
                },
                function (Throwable $e) use ($promise): void {
                    $this->activeConnections--;
                    $promise->reject($e);
                }
            )
        ;

        return $promise;
    }

    /**
     * Creates a new connection specifically to satisfy the next queued waiter.
     */
    private function createConnectionForWaiter(): void
    {
        $waiter = $this->dequeueActiveWaiter();

        if ($waiter === null) {
            return;
        }

        $this->activeConnections++;

        MysqlConnection::create($this->connectionParams, $this->connector)
            ->then(
                function (MysqlConnection $connection) use ($waiter): void {
                    $connId = spl_object_id($connection);
                    $this->connectionCreatedAt[$connId] = (int) hrtime(true);

                    // Mark as active.
                    $this->activeConnectionsMap[$connId] = $connection;

                    if ($waiter->isCancelled()) {
                        $this->releaseClean($connection);

                        return;
                    }

                    $waiter->resolve($connection);
                },
                function (Throwable $e) use ($waiter): void {
                    $this->activeConnections--;
                    $waiter->reject($e);
                }
            )
        ;
    }

    /**
     * Dequeues the next valid waiter promise.
     *
     * Discards any waiters that are:
     * 1. Cancelled (by the user)
     * 2. Rejected (by acquire timeout)
     *
     * @return Promise<MysqlConnection>|null
     */
    private function dequeueActiveWaiter(): ?Promise
    {
        while (! $this->waiters->isEmpty()) {
            /** @var Promise<MysqlConnection> $waiter */
            $waiter = $this->waiters->dequeue();

            // Check isSettled() instead of just isCancelled().
            // This properly handles both "Cancelled by user" and "Rejected by Timeout".
            if ($waiter->isPending()) {
                return $waiter;
            }
        }

        return null;
    }

    /**
     * Satisfies the next waiter if pool capacity allows after a connection
     * is removed (e.g. health check failure, idle timeout eviction).
     */
    private function satisfyNextWaiter(): void
    {
        if (! $this->waiters->isEmpty() && $this->activeConnections < $this->maxSize) {
            $this->createConnectionForWaiter();
        }
    }

    /**
     * Closes and removes a connection, cleaning up all tracking metadata.
     */
    private function removeConnection(MysqlConnection $connection): void
    {
        if (! $connection->isClosed()) {
            $connection->close();
        }

        $connId = spl_object_id($connection);
        unset(
            $this->connectionLastUsed[$connId],
            $this->connectionCreatedAt[$connId],
            $this->drainingConnections[$connId],
            $this->activeConnectionsMap[$connId]
        );

        $this->activeConnections--;
    }

    public function __destruct()
    {
        $this->close();
    }
}
