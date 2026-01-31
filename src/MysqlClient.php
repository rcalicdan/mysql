<?php

declare(strict_types=1);

namespace Hibla\Mysql;

use Hibla\Cache\ArrayCache;
use Hibla\Mysql\Enums\IsolationLevel;
use Hibla\Mysql\Exceptions\ConfigurationException;
use Hibla\Mysql\Exceptions\NotInitializedException;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Internals\ExecuteResult;
use Hibla\Mysql\Internals\PreparedStatement;
use Hibla\Mysql\Internals\QueryResult;
use Hibla\Mysql\Internals\Transaction;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;

use function Hibla\async;
use function Hibla\await;

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

    /** @var \WeakMap<Connection, ArrayCache>|null Statement caches per connection */
    private ?\WeakMap $statementCaches = null;

    /** @var int Maximum number of prepared statements to cache per connection */
    private int $statementCacheSize;

    /** @var bool Whether statement caching is enabled */
    private bool $enableStatementCache;

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
     * @param int $maxConnections Maximum number of connections in the pool (default: 10)
     * @param int $idleTimeout Seconds a connection can remain idle before being closed (default: 60)
     * @param int $maxLifetime Maximum seconds a connection can live before being rotated (default: 3600)
     * @param int $statementCacheSize Maximum number of prepared statements to cache per connection (default: 256)
     * @param bool $enableStatementCache Whether to enable prepared statement caching (default: true)
     *
     * @throws ConfigurationException If configuration is invalid
     */
    public function __construct(
        ConnectionParams|array|string $config,
        int $maxConnections = 10,
        int $idleTimeout = 60,
        int $maxLifetime = 3600,
        int $statementCacheSize = 256,
        bool $enableStatementCache = true
    ) {
        try {
            $this->pool = new PoolManager(
                $config,
                $maxConnections,
                $idleTimeout,
                $maxLifetime
            );
            $this->statementCacheSize = $statementCacheSize;
            $this->enableStatementCache = $enableStatementCache;
            
            if ($this->enableStatementCache) {
                $this->statementCaches = new \WeakMap();
            }
            
            $this->isInitialized = true;
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
        $this->statementCaches = null;
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
            })
        ;
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

                if (\count($params) === 0) {
                    return $conn->query($sql);
                }

                if ($this->enableStatementCache) {
                    return $this->getCachedStatement($conn, $sql)
                        ->then(function (PreparedStatement $stmt) use ($params) {
                            return $stmt->executeStatement($params);
                        });
                }

                /** @var PreparedStatement|null $stmtRef */
                $stmtRef = null;

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                        $stmtRef = $stmt;
                        return $stmt->executeStatement($params);
                    })
                    ->finally(function () use (&$stmtRef) {
                        if ($stmtRef !== null) {
                            return $stmtRef->close();
                        }
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
     * @return PromiseInterface<ExecuteResult> Promise resolving to execution result
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

                if (\count($params) === 0) {
                    return $conn->execute($sql);
                }

                if ($this->enableStatementCache) {
                    return $this->getCachedStatement($conn, $sql)
                        ->then(function (PreparedStatement $stmt) use ($params) {
                            return $stmt->executeStatement($params);
                        });
                }

                /** @var PreparedStatement|null $stmtRef */
                $stmtRef = null;

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                        $stmtRef = $stmt;
                        return $stmt->executeStatement($params);
                    })
                    ->finally(function () use (&$stmtRef) {
                        if ($stmtRef !== null) {
                            return $stmtRef->close();
                        }
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
            })
        ;
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
            })
        ;
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
            })
        ;
    }

    /**
     * Begins a database transaction manually.
     *
     * Returns a Transaction that automatically releases the connection
     * back to the pool when commit() or rollback() is called.
     *
     * The Transaction object has all the same methods as MysqlClient
     * (query, execute, fetchOne, fetchValue, prepare) for convenience.
     *
     * Example:
     * ```php
     * $tx = await($client->beginTransaction());
     * await($tx->execute('INSERT INTO users (name) VALUES (?)', ['Alice']));
     * await($tx->commit());
     * ```
     *
     * For automatic transaction management with auto-commit/rollback,
     * use the transaction() method instead.
     *
     * @param IsolationLevel|null $isolationLevel Optional transaction isolation level
     * @return PromiseInterface<Transaction> Promise resolving to transaction object
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function beginTransaction(?IsolationLevel $isolationLevel = null): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use ($isolationLevel, $pool, &$connection) {
                $connection = $conn;

                return $conn->beginTransaction($isolationLevel, $pool);
            })
            ->catch(function (\Throwable $e) use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }

                throw $e;
            });
    }

    /**
     * Executes a callback within a database transaction with automatic management and retries.
     *
     * The callback is automatically wrapped in a Fiber.
     * If the callback or commit fails, the transaction is rolled back.
     * If $attempts > 1, the process (Begin -> Callback -> Commit) is retried.
     *
     * @template TResult
     *
     * @param callable(Transaction): (PromiseInterface<TResult>|TResult) $callback 
     * @param int $attempts Number of times to attempt the transaction (default: 1)
     * @param IsolationLevel|null $isolationLevel 
     * @return PromiseInterface<TResult>
     *
     * @throws NotInitializedException
     * @throws \Throwable The final exception if all attempts fail
     */
    public function transaction(
        callable $callback,
        int $attempts = 1,
        ?IsolationLevel $isolationLevel = null
    ): PromiseInterface {
        if ($attempts < 1) {
            throw new \InvalidArgumentException("Attempts must be at least 1");
        }

        return async(function () use ($callback, $attempts, $isolationLevel) {
            $lastError = null;

            for ($attempt = 1; $attempt <= $attempts; $attempt++) {
                $tx = null;

                try {
                    /** @var Transaction $tx */
                    $tx = await($this->beginTransaction($isolationLevel));

                    $result = await(async(fn() => $callback($tx)));

                    await($tx->commit());

                    return $result;
                } catch (\Throwable $e) {
                    $lastError = $e;

                    if ($tx !== null && $tx->isActive()) {
                        try {
                            await($tx->rollback());
                        } catch (\Throwable $rollbackError) {
                            // If rollback fails (e.g. connection lost), we continue to the retry logic
                            // but keep the original error ($e) as the primary cause.
                        }
                    }

                    if ($attempt === $attempts) {
                        break;
                    }
                }
            }

            throw $lastError;
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
     *                                  - statement_cache_enabled: Whether statement caching is enabled
     *                                  - statement_cache_size: Maximum statements per connection
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getStats(): array
    {
        $stats = $this->getPool()->getStats();
        
        $stats['statement_cache_enabled'] = $this->enableStatementCache;
        $stats['statement_cache_size'] = $this->statementCacheSize;
        
        return $stats;
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
     * Clears the prepared statement cache for all connections.
     * 
     * This is useful for:
     * - Testing different caching strategies
     * - Freeing memory when needed
     * - Forcing re-preparation of statements
     *
     * @return void
     */
    public function clearStatementCache(): void
    {
        if ($this->statementCaches !== null) {
            $this->statementCaches = new \WeakMap();
        }
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
        if (! $this->isInitialized) {
            return;
        }

        if ($this->pool !== null) {
            $this->pool->close();
            $this->pool = null;
        }

        $this->statementCaches = null;
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
        $this->close();
    }

    /**
     * Gets a prepared statement from cache or creates and caches a new one.
     *
     * This method implements the prepared statement caching logic:
     * 1. Check if statement exists in cache for this connection
     * 2. If found, return cached statement (cache hit)
     * 3. If not found, prepare new statement and cache it (cache miss)
     *
     * Statements are cached per-connection because prepared statements
     * are session-scoped in MySQL.
     *
     * @param Connection $conn The connection to prepare the statement on
     * @param string $sql The SQL query with ? placeholders
     * @return PromiseInterface<PreparedStatement> Promise resolving to prepared statement
     */
    private function getCachedStatement(Connection $conn, string $sql): PromiseInterface
    {
        // Get or create cache for this connection
        if (!isset($this->statementCaches[$conn])) {
            $this->statementCaches[$conn] = new ArrayCache($this->statementCacheSize);
        }

        $cache = $this->statementCaches[$conn];

        return $cache->get($sql)
            ->then(function ($stmt) use ($conn, $sql, $cache) {
                if ($stmt instanceof PreparedStatement) {
                    // Cache hit - reuse existing prepared statement
                    return Promise::resolved($stmt);
                }

                // Cache miss - prepare and cache the statement
                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($sql, $cache) {
                        // Cache the prepared statement (fire and forget)
                        $cache->set($sql, $stmt);
                        return $stmt;
                    });
            });
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
        if (! $this->isInitialized || $this->pool === null) {
            throw new NotInitializedException(
                'MysqlClient instance has not been initialized or has been reset.'
            );
        }

        return $this->pool;
    }
}