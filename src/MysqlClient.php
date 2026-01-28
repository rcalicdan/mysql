<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Exceptions\ConfigurationException;
use Hibla\MysqlClient\Exceptions\NotInitializedException;
use Hibla\MysqlClient\Internals\Connection;
use Hibla\MysqlClient\Internals\ExecuteResult;
use Hibla\MysqlClient\Internals\PreparedStatement;
use Hibla\MysqlClient\Internals\QueryResult;
use Hibla\MysqlClient\Manager\PoolManager;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\MysqlClient\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Rcalicdan\Defer\Defer;

/**
 * Instance-based Asynchronous MySQL Client with Connection Pooling.
 *
 * This class provides a high-level API for managing MySQL database connections.
 * Each instance is completely independent, allowing true multi-database support
 * without global state.
 */
final class MysqlClient
{
    /** @var PoolManager|null Connection pool instance for this client */
    private ?PoolManager $pool = null;

    /** @var bool Tracks initialization state of this instance */
    private bool $isInitialized = false;

    /**
     * Creates a new independent MysqlClient instance.
     *
     * Each instance manages its own connection pool and is completely
     * independent from other instances, allowing true multi-database support.
     *
     * @param ConnectionParams|array<string, mixed>|string $config Database configuration:
     *        - ConnectionParams object, or
     *        - Array with keys: host, port, user, password, database, or
     *        - DSN string: mysql://user:password@host:port/database
     * @param int $poolSize Maximum number of connections in the pool
     *
     * @throws ConfigurationException If configuration is invalid
     */
    public function __construct(
        ConnectionParams|array|string $config,
        int $poolSize = 10
    ) {
        try {
            $this->pool = new PoolManager($config, $poolSize);
            $this->isInitialized = true;

            register_shutdown_function($this->close(...));
        } catch (\InvalidArgumentException $e) {
            throw new ConfigurationException(
                'Invalid database configuration: ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * Resets this instance, closing all connections and clearing state.
     * After reset, this instance cannot be used until recreated.
     *
     * @return void
     */
    public function reset(): void
    {
        if ($this->pool !== null) {
            $this->pool->close();
        }
        $this->pool = null;
        $this->isInitialized = false;
    }

    /**
     * Executes a callback with a connection from this instance's pool.
     *
     * Automatically handles connection acquisition and release. The callback
     * receives a Connection instance and can perform any database operations.
     * The connection is guaranteed to be released back to the pool even if
     * the callback throws an exception.
     *
     * @template TResult
     *
     * @param callable(Connection): (PromiseInterface<TResult>|TResult) $callback Function that receives Connection instance
     * @return PromiseInterface<TResult> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function run(callable $callback): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use ($callback, &$connection) {
                $connection = $conn;
                $result = $callback($conn);

                if ($result instanceof PromiseInterface) {
                    return $result;
                }

                return Promise::resolved($result);
            })
            ->finally(function () use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            });
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * The query is executed asynchronously. For small to medium result sets,
     * all rows are buffered in memory. For large result sets, use streamQuery().
     * 
     * If parameters are provided, the query is automatically prepared as a
     * prepared statement before execution to prevent SQL injection.
     *
     * @param string $sql SQL query to execute with optional ? placeholders
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<QueryResult> Promise resolving to query result
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use ($sql, $params, &$connection) {
                $connection = $conn;

                if (empty($params)) {
                    return $conn->query($sql);
                }

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($params) {
                        return $stmt->executeStatement($params);
                    });
            })
            ->finally(function () use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            });
    }

    /**
     * Executes a SQL statement (INSERT, UPDATE, DELETE, etc.).
     * 
     * If parameters are provided, the statement is automatically prepared as a
     * prepared statement before execution to prevent SQL injection.
     *
     * @param string $sql SQL statement to execute with optional ? placeholders
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<ExecuteResult|QueryResult> Promise resolving to execution result
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use ($sql, $params, &$connection) {
                $connection = $conn;

                if (empty($params)) {
                    return $conn->execute($sql);
                }

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($params) {
                        return $stmt->executeStatement($params);
                    });
            })
            ->finally(function () use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            });
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * Returns null if no rows match the query.
     * If parameters are provided, the query is automatically prepared as a
     * prepared statement before execution to prevent SQL injection.
     *
     * @param string $sql SQL query to execute with optional ? placeholders
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<array<string, mixed>|null> Promise resolving to associative array or null
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(function (QueryResult $result) {
                return $result->fetchOne();
            });
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * Useful for queries that return a single scalar value like COUNT, MAX, etc.
     * Returns null if the query returns no rows.
     * If parameters are provided, the query is automatically prepared as a
     * prepared statement before execution to prevent SQL injection.
     *
     * @param string $sql SQL query to execute with optional ? placeholders
     * @param string|int $column Column name or index (default: 0)
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<mixed> Promise resolving to scalar value or null
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function fetchValue(string $sql, string|int $column = 0, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(function (QueryResult $result) use ($column) {
                $row = $result->fetchOne();
                if ($row === null) {
                    return null;
                }
                return $row[$column] ?? null;
            });
    }

    /**
     * Stream a SELECT query row-by-row without buffering in memory.
     *
     * This is ideal for processing large result sets that may exceed available memory.
     *
     * @param string $sql The SQL SELECT query to execute
     * @param callable(array<string, mixed>): void $onRow Callback invoked for each row
     * @param callable(StreamStats): void|null $onComplete Optional callback when streaming completes
     * @param callable(\Throwable): void|null $onError Optional callback for error handling
     * @return PromiseInterface<StreamStats> Resolves with streaming statistics
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function streamQuery(
        string $sql,
        callable $onRow,
        ?callable $onComplete = null,
        ?callable $onError = null
    ): PromiseInterface {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use ($sql, $onRow, $onComplete, $onError, &$connection) {
                $connection = $conn;
                return $conn->streamQuery($sql, $onRow, $onComplete, $onError);
            })
            ->finally(function () use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            });
    }

    /**
     * Pings the server to check if a connection is alive.
     *
     * @return PromiseInterface<bool> Promise resolving to true if connection is alive
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function ping(): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use (&$connection) {
                $connection = $conn;
                return $conn->ping();
            })
            ->finally(function () use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            });
    }

    /**
     * Performs a health check on all idle connections in the pool.
     *
     * @return PromiseInterface<array<string, int>> Promise resolving to health statistics
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function healthCheck(): PromiseInterface
    {
        return $this->getPool()->healthCheck();
    }

    /**
     * Gets statistics about this instance's connection pool.
     *
     * @return array<string, int|bool> Pool statistics including:
     *                                  - active_connections: Total connections created
     *                                  - pooled_connections: Available connections
     *                                  - waiting_requests: Requests waiting for connection
     *                                  - max_size: Maximum pool size
     *                                  - config_validated: Whether config was validated
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getStats(): array
    {
        return $this->getPool()->getStats();
    }

    /**
     * Gets the most recently used connection from this pool.
     *
     * This is primarily useful for debugging and testing purposes.
     * Returns null if no connection has been used yet.
     *
     * @return Connection|null The last connection or null if none used yet
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getLastConnection(): ?Connection
    {
        return $this->getPool()->getLastConnection();
    }

    /**
     * Closes all connections and shuts down the pool.
     *
     * After calling close(), this instance cannot be used until recreated.
     * This method ensures all resources are properly released and the event
     * loop can exit cleanly.
     *
     * @return void
     */
    public function close(): void
    {
        if (!$this->isInitialized) {
            return;
        }

        if ($this->pool !== null) {
            $this->pool->close();
            $this->pool = null;
        }

        $this->isInitialized = false;
    }

    /**
     * Destructor ensures cleanup on object destruction.
     * 
     * Note: While this helps with cleanup, it's better practice to explicitly
     * call close() when you're done with the client to ensure immediate cleanup
     * and proper event loop shutdown.
     *
     * @return void
     */
    public function __destruct()
    {
        echo "mysql client destruct called\n";
        $this->close();
    }

    /**
     * Gets the connection pool instance.
     *
     * @return PoolManager The initialized connection pool
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    private function getPool(): PoolManager
    {
        if (!$this->isInitialized || $this->pool === null) {
            throw new NotInitializedException(
                'MysqlClient instance has not been initialized or has been reset.'
            );
        }

        return $this->pool;
    }
}
