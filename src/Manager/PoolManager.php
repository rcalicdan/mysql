<?php

declare(strict_types=1);

namespace Hibla\Mysql\Manager;

use Hibla\Mysql\Exceptions\PoolException;
use Hibla\Mysql\Internals\Connection as MysqlConnection;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
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
 * This class is not subject to any backward compatibility (BC) guarantees. Its
 * methods, properties, and overall behavior may change without notice in any
 * patch, minor, or major version.
 *
 * @see \Hibla\Mysql\MysqlClient
 */
class PoolManager
{
    /**
     * @var SplQueue<MysqlConnection> A queue of available, idle connections.
     */
    private SplQueue $pool;

    /**
     * @var SplQueue<Promise<MysqlConnection>> A queue of pending requests (waiters) for a connection.
     */
    private SplQueue $waiters;

    /**
     * @var int The maximum number of concurrent connections allowed.
     */
    private int $maxSize;

    /**
     * @var int The current number of active connections (both in pool and in use).
     */
    private int $activeConnections = 0;

    /**
     * @var ConnectionParams The database connection configuration.
     */
    private ConnectionParams $connectionParams;

    /**
     * @var MysqlConnection|null The most recently used or created connection.
     */
    private ?MysqlConnection $lastConnection = null;

    /**
     * @var bool Flag indicating if the initial configuration was validated.
     */
    private bool $configValidated = false;

    /**
     * @var int Idle timeout in nanoseconds.
     */
    private int $idleTimeoutNanos;

    /**
     * @var int Max lifetime in nanoseconds.
     */
    private int $maxLifetimeNanos;

    /**
     * @var array<int, int> Tracks last usage timestamp (hrtime) for each connection.
     */
    private array $connectionLastUsed = [];

    /**
     * @var array<int, int> Tracks creation timestamp (hrtime) for each connection.
     */
    private array $connectionCreatedAt = [];

    /**
     * Creates a new MySQL connection pool.
     *
     * @param ConnectionParams|array<string, mixed>|string $config The database connection parameters.
     * @param int $maxSize The maximum number of connections this pool can manage.
     * @param int $idleTimeout Seconds a connection can stay idle before closure (default: 300 / 5 mins).
     * @param int $maxLifetime Seconds a connection can live in total before rotation (default: 3600 / 1 hour).
     *
     * @throws InvalidArgumentException If the database configuration is invalid.
     */
    public function __construct(
        ConnectionParams|array|string $config,
        int $maxSize = 10,
        int $idleTimeout = 300,
        int $maxLifetime = 3600
    ) {
        $this->connectionParams = match (true) {
            $config instanceof ConnectionParams => $config,
            \is_array($config) => ConnectionParams::fromArray($config),
            \is_string($config) => ConnectionParams::fromUri($config),
        };

        if ($maxSize <= 0) {
            throw new InvalidArgumentException('Pool max size must be greater than 0');
        }

        if ($idleTimeout <= 0) {
            throw new InvalidArgumentException('Idle timeout must be greater than 0');
        }

        if ($maxLifetime <= 0) {
            throw new InvalidArgumentException('Max lifetime must be greater than 0');
        }

        $this->configValidated = true;
        $this->maxSize = $maxSize;

        $this->idleTimeoutNanos = $idleTimeout * 1_000_000_000;
        $this->maxLifetimeNanos = $maxLifetime * 1_000_000_000;

        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
    }

    /**
     * Asynchronously acquires a connection from the pool.
     *
     * Uses "Check-on-Borrow" strategy:
     * 1. Checks if the idle connection has exceeded idle timeout.
     * 2. Checks if the connection has exceeded max lifetime.
     * 3. Checks if the connection is still alive (TCP state).
     *
     * @return PromiseInterface<MysqlConnection> A promise that resolves with a database connection.
     */
    public function get(): PromiseInterface
    {
        while (! $this->pool->isEmpty()) {
            /** @var MysqlConnection $connection */
            $connection = $this->pool->dequeue();

            $connId = spl_object_id($connection);
            $now = hrtime(true);

            $lastUsed = $this->connectionLastUsed[$connId] ?? 0;
            $createdAt = $this->connectionCreatedAt[$connId] ?? 0;

            // 1. Check Idle Timeout
            if (($now - $lastUsed) > $this->idleTimeoutNanos) {
                $this->removeConnection($connection);

                continue;
            }

            // 2. Check Max Lifetime
            if (($now - $createdAt) > $this->maxLifetimeNanos) {
                $this->removeConnection($connection);

                continue;
            }

            // 3. Check Connection Health (TCP State)
            if (! $connection->isReady() || $connection->isClosed()) {
                $this->removeConnection($connection);

                continue;
            }

            // Valid Connection Found!
            // Remove from idle tracking (it's active now)
            unset($this->connectionLastUsed[$connId]);

            // Resume the connection (re-attach event loop listeners)
            $connection->resume();
            $this->lastConnection = $connection;

            return Promise::resolved($connection);
        }

        // If here: pool is empty or we discarded all stale connections.
        if ($this->activeConnections < $this->maxSize) {
            $this->activeConnections++;

            $promise = new Promise();

            MysqlConnection::create($this->connectionParams)
                ->then(
                    function (MysqlConnection $connection) use ($promise) {
                        // Track creation time for Max Lifetime
                        $connId = spl_object_id($connection);
                        $this->connectionCreatedAt[$connId] = hrtime(true);

                        $this->lastConnection = $connection;
                        $promise->resolve($connection);
                    },
                    function (Throwable $e) use ($promise) {
                        $this->activeConnections--;
                        $promise->reject($e);
                    }
                )
            ;

            return $promise;
        }

        // Queue the request if at capacity
        /** @var Promise<MysqlConnection> $promise */
        $promise = new Promise();
        $this->waiters->enqueue($promise);

        return $promise;
    }

    /**
     * Releases a connection back to the pool.
     *
     * @param MysqlConnection $connection The connection to release.
     */
    public function release(MysqlConnection $connection): void
    {
        // Check if connection is still alive
        if ($connection->isClosed() || ! $connection->isReady()) {
            $this->removeConnection($connection);

            if (! $this->waiters->isEmpty() && $this->activeConnections < $this->maxSize) {
                $this->createConnectionForWaiter();
            }

            return;
        }

        // Prioritize giving the connection to a waiting request
        if (! $this->waiters->isEmpty()) {
            /** @var Promise<MysqlConnection> $promise */
            $promise = $this->waiters->dequeue();
            $this->lastConnection = $connection;
            // Connection stays active (resumed) for the waiter
            $promise->resolve($connection);
        } else {
            // No waiters. Pause the connection to let the loop exit if idle.
            $connection->pause();

            $connId = spl_object_id($connection);
            $now = hrtime(true);

            // If it expired while being used, discard it now rather than later.
            $createdAt = $this->connectionCreatedAt[$connId] ?? 0;
            if (($now - $createdAt) > $this->maxLifetimeNanos) {
                $this->removeConnection($connection);

                return;
            }

            // Track last usage time for Idle Timeout
            $this->connectionLastUsed[$connId] = $now;

            $this->pool->enqueue($connection);
        }
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
            'waiting_requests' => $this->waiters->count(),
            'max_size' => $this->maxSize,
            'config_validated' => $this->configValidated,
            'tracked_connections' => count($this->connectionCreatedAt),
        ];
    }

    /**
     * Closes all connections and shuts down the pool.
     */
    public function close(): void
    {
        while (! $this->pool->isEmpty()) {
            $connection = $this->pool->dequeue();
            if (! $connection->isClosed()) {
                $connection->close();
            }
        }

        while (! $this->waiters->isEmpty()) {
            /** @var Promise<MysqlConnection> $promise */
            $promise = $this->waiters->dequeue();
            $promise->reject(new PoolException('Pool is being closed'));
        }

        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
        $this->activeConnections = 0;
        $this->lastConnection = null;

        $this->connectionLastUsed = [];
        $this->connectionCreatedAt = [];
    }

    /**
     * Pings all idle connections in the pool to check their health.
     *
     * @return PromiseInterface<array<string, int>> A promise that resolves with health statistics.
     */
    public function healthCheck(): PromiseInterface
    {
        $promise = new Promise();
        $stats = [
            'total_checked' => 0,
            'healthy' => 0,
            'unhealthy' => 0,
        ];

        $tempQueue = new SplQueue();
        $checkPromises = [];

        // Check all connections currently in the pool
        while (! $this->pool->isEmpty()) {
            /** @var MysqlConnection $connection */
            $connection = $this->pool->dequeue();
            $stats['total_checked']++;

            $connection->resume();

            $checkPromises[] = $connection->ping()
                ->then(
                    function () use ($connection, $tempQueue, &$stats) {
                        $stats['healthy']++;
                        $connection->pause();

                        $connId = spl_object_id($connection);
                        $this->connectionLastUsed[$connId] = hrtime(true);

                        $tempQueue->enqueue($connection);
                    },
                    function () use ($connection, &$stats) {
                        $stats['unhealthy']++;
                        $this->removeConnection($connection);
                    }
                )
            ;
        }

        // Wait for all pings to complete
        Promise::all($checkPromises)
            ->then(
                function () use ($promise, $tempQueue, &$stats) {
                    // Return healthy connections back to pool
                    while (! $tempQueue->isEmpty()) {
                        $this->pool->enqueue($tempQueue->dequeue());
                    }
                    $promise->resolve($stats);
                },
                function (Throwable $e) use ($promise, $tempQueue, &$stats) {
                    // Return any remaining connections back to pool
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
     * Removes and closes a connection, cleaning up all tracking data.
     *
     * @param MysqlConnection $connection The connection to remove.
     * @return void
     */
    private function removeConnection(MysqlConnection $connection): void
    {
        if (! $connection->isClosed()) {
            $connection->close();
        }

        $connId = spl_object_id($connection);
        unset($this->connectionLastUsed[$connId]);
        unset($this->connectionCreatedAt[$connId]);

        $this->activeConnections--;
    }

    /**
     * Creates a new connection for a waiting request.
     *
     * @return void
     */
    private function createConnectionForWaiter(): void
    {
        $this->activeConnections++;

        /** @var Promise<MysqlConnection> $promise */
        $promise = $this->waiters->dequeue();

        MysqlConnection::create($this->connectionParams)
            ->then(
                function (MysqlConnection $newConnection) use ($promise) {
                    // Track creation time for Max Lifetime
                    $connId = spl_object_id($newConnection);
                    $this->connectionCreatedAt[$connId] = hrtime(true);

                    $this->lastConnection = $newConnection;
                    $promise->resolve($newConnection);
                },
                function (Throwable $e) use ($promise) {
                    $this->activeConnections--;
                    $promise->reject($e);
                }
            )
        ;
    }

    public function __destruct()
    {
        $this->close();
    }
}
