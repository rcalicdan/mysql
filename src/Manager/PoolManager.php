<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Manager;

use Hibla\MysqlClient\Exceptions\PoolException;
use Hibla\MysqlClient\Internals\Connection as MysqlConnection;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use InvalidArgumentException;
use SplQueue;
use Throwable;

/**
 * Manages a pool of asynchronous MySQL connections.
 *
 * This class provides a robust mechanism for managing a limited number of database
 * connections, preventing resource exhaustion. It integrates with the Event Loop
 * to allow automatic script termination when connections are idle.
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
     * Creates a new MySQL connection pool.
     *
     * @param ConnectionParams|array<string, mixed>|string $config The database connection parameters.
     * @param int $maxSize The maximum number of connections this pool can manage.
     *
     * @throws InvalidArgumentException If the database configuration is invalid.
     */
    public function __construct(ConnectionParams|array|string $config, int $maxSize = 10)
    {
        $this->connectionParams = match (true) {
            $config instanceof ConnectionParams => $config,
            \is_array($config) => ConnectionParams::fromArray($config),
            \is_string($config) => ConnectionParams::fromUri($config),
        };

        if ($maxSize <= 0) {
            throw new InvalidArgumentException('Pool max size must be greater than 0');
        }

        $this->configValidated = true;
        $this->maxSize = $maxSize;
        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
    }

    /**
     * Asynchronously acquires a connection from the pool.
     *
     * @return PromiseInterface<MysqlConnection> A promise that resolves with a database connection.
     */
    public function get(): PromiseInterface
    {
        // Check for idle connection
        if (!$this->pool->isEmpty()) {
            /** @var MysqlConnection $connection */
            $connection = $this->pool->dequeue();
            
            // Verify connection is still alive
            if ($connection->isReady()) {
                // Resume the connection (re-attach event loop listeners) so it can receive data
                $connection->resume();
                
                $this->lastConnection = $connection;
                return Promise::resolved($connection);
            }
            
            // Connection is dead, decrement counter and continue
            $this->activeConnections--;
        }

        // Create new connection if under capacity
        if ($this->activeConnections < $this->maxSize) {
            $this->activeConnections++;

            $promise = new Promise();
            
            MysqlConnection::create($this->connectionParams)
                ->then(
                    function (MysqlConnection $connection) use ($promise) {
                        // New connections are created in 'active' state (resumed), so we use them directly
                        $this->lastConnection = $connection;
                        $promise->resolve($connection);
                    },
                    function (Throwable $e) use ($promise) {
                        $this->activeConnections--;
                        $promise->reject($e);
                    }
                );

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
        if ($connection->isClosed() || !$connection->isReady()) {
            $this->activeConnections--;
            
            // If a waiter exists and we have capacity, create a new connection for them
            if (!$this->waiters->isEmpty() && $this->activeConnections < $this->maxSize) {
                $this->activeConnections++;
                /** @var Promise<MysqlConnection> $promise */
                $promise = $this->waiters->dequeue();

                MysqlConnection::create($this->connectionParams)
                    ->then(
                        function (MysqlConnection $newConnection) use ($promise) {
                            $this->lastConnection = $newConnection;
                            $promise->resolve($newConnection);
                        },
                        function (Throwable $e) use ($promise) {
                            $this->activeConnections--;
                            $promise->reject($e);
                        }
                    );
            }

            return;
        }

        // Prioritize giving the connection to a waiting request
        if (!$this->waiters->isEmpty()) {
            /** @var Promise<MysqlConnection> $promise */
            $promise = $this->waiters->dequeue();
            $this->lastConnection = $connection;
            $promise->resolve($connection);
        } else {
            $connection->pause();
            $this->pool->enqueue($connection);
        }
    }

    /**
     * Gets the most recently active connection handled by the pool.
     *
     * @return MysqlConnection|null The last connection object or null if none have been handled.
     */
    public function getLastConnection(): ?MysqlConnection
    {
        return $this->lastConnection;
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
        ];
    }

    /**
     * Closes all connections and shuts down the pool.
     */
    public function close(): void
    {
        // Close all idle connections
        while (!$this->pool->isEmpty()) {
            $connection = $this->pool->dequeue();
            if (!$connection->isClosed()) {
                $connection->close();
            }
        }
        
        // Reject all waiting requests
        while (!$this->waiters->isEmpty()) {
            /** @var Promise<MysqlConnection> $promise */
            $promise = $this->waiters->dequeue();
            $promise->reject(new PoolException('Pool is being closed'));
        }
        
        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
        $this->activeConnections = 0;
        $this->lastConnection = null;
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
        while (!$this->pool->isEmpty()) {
            /** @var MysqlConnection $connection */
            $connection = $this->pool->dequeue();
            $stats['total_checked']++;

            $connection->resume();

            $checkPromises[] = $connection->ping()
                ->then(
                    function () use ($connection, $tempQueue, &$stats) {
                        $stats['healthy']++;
                        $connection->pause();
                        $tempQueue->enqueue($connection);
                    },
                    function () use ($connection, &$stats) {
                        $stats['unhealthy']++;
                        $this->activeConnections--;
                        if (!$connection->isClosed()) {
                            $connection->close();
                        }
                    }
                );
        }

        // Wait for all pings to complete
        Promise::all($checkPromises)
            ->then(
                function () use ($promise, $tempQueue, &$stats) {
                    // Return healthy connections back to pool
                    while (!$tempQueue->isEmpty()) {
                        $this->pool->enqueue($tempQueue->dequeue());
                    }
                    $promise->resolve($stats);
                },
                function (Throwable $e) use ($promise, $tempQueue, &$stats) {
                    // Return any remaining connections back to pool
                    while (!$tempQueue->isEmpty()) {
                        $this->pool->enqueue($tempQueue->dequeue());
                    }
                    $promise->reject($e);
                }
            );

        return $promise;
    }

    public function __destruct()
    {
        $this->close();
    }
}