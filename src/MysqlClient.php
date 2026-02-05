<?php

declare(strict_types=1);

namespace Hibla\Mysql;

use function Hibla\async;
use function Hibla\await;

use Hibla\Cache\ArrayCache;
use Hibla\Mysql\Enums\IsolationLevel;
use Hibla\Mysql\Exceptions\ConfigurationException;
use Hibla\Mysql\Exceptions\NotInitializedException;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Internals\ManagedPreparedStatement;
use Hibla\Mysql\Internals\PreparedStatement;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\Internals\Transaction;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;

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
     * @param ConnectionParams|array<string, mixed>|string $config Database configuration
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
     * Prepares a SQL statement for multiple executions.
     *
     * @param string $sql SQL query with ? placeholders
     * @return PromiseInterface<ManagedPreparedStatement>
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function prepare(string $sql): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $pool->get()
            ->then(function (Connection $conn) use ($sql, $pool, &$connection) {
                $connection = $conn;

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($conn, $pool) {
                        return new ManagedPreparedStatement($stmt, $conn, $pool);
                    })
                ;
            })
            ->catch(function (\Throwable $e) use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }

                throw $e;
            })
        ;
    }

    /**
     * Executes any SQL statement and returns full Result object.
     *
     * @param string $sql SQL statement to execute
     * @param array<int, mixed> $params Optional parameters
     * @return PromiseInterface<Result>
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
                            return $stmt->execute($params);
                        })
                    ;
                }

                /** @var PreparedStatement|null $stmtRef */
                $stmtRef = null;

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                        $stmtRef = $stmt;

                        return $stmt->execute($params);
                    })
                    ->finally(function () use (&$stmtRef) {
                        if ($stmtRef !== null) {
                            return $stmtRef->close();
                        }
                    })
                ;
            })
            ->finally(function () use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            })
        ;
    }

    /**
     * Executes a SQL statement and returns the number of affected rows.
     *
     * @param string $sql SQL statement to execute
     * @param array<int, mixed> $params Optional parameters
     * @return PromiseInterface<int>
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(fn (Result $result) => $result->getAffectedRows())
        ;
    }

    /**
     * Executes a SQL statement and returns the last inserted auto-increment ID.
     *
     * @param string $sql SQL statement to execute
     * @param array<int, mixed> $params Optional parameters
     * @return PromiseInterface<int>
     */
    public function executeGetId(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(fn (Result $result) => $result->getLastInsertId())
        ;
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param string $sql SQL query to execute
     * @param array<int, mixed> $params Optional parameters
     * @return PromiseInterface<array<string, mixed>|null>
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(fn (Result $result) => $result->fetchOne())
        ;
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * @param string $sql SQL query to execute
     * @param string|int $column Column name or index (default: 0)
     * @param array<int, mixed> $params Optional parameters
     * @return PromiseInterface<mixed>
     */
    public function fetchValue(string $sql, string|int $column = 0, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(function (Result $result) use ($column) {
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
     * @param string $sql The SQL SELECT query to execute
     * @param callable(array<string, mixed>): void $onRow Callback invoked for each row
     * @param callable(StreamStats): void|null $onComplete Optional callback when streaming completes
     * @param callable(\Throwable): void|null $onError Optional callback for error handling
     * @return PromiseInterface<StreamStats>
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
     * Begins a database transaction with automatic connection pool management.
     *
     * Returns a Transaction that automatically releases the connection
     * back to the pool when commit() or rollback() is called.
     *
     * Example:
     * ```php
     * $tx = await($client->beginTransaction());
     * await($tx->query('INSERT INTO users (name) VALUES (?)', ['Alice']));
     * await($tx->commit()); // Auto-releases connection to pool
     * ```
     *
     * @param IsolationLevel|null $isolationLevel Optional transaction isolation level
     * @return PromiseInterface<Transaction>
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

                $sql = $isolationLevel !== null
                    ? "SET TRANSACTION ISOLATION LEVEL {$isolationLevel->value}; START TRANSACTION"
                    : 'START TRANSACTION';

                return $conn->query($sql)
                    ->then(function () use ($conn, $pool) {
                        return new Transaction($conn, $pool);
                    })
                ;
            })
            ->catch(function (\Throwable $e) use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }

                throw $e;
            })
        ;
    }

    /**
     * Executes a callback within a database transaction with automatic management and retries.
     *
     * @template TResult
     *
     * @param callable(Transaction): (TResult) $callback
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
            throw new \InvalidArgumentException('Attempts must be at least 1');
        }

        return async(function () use ($callback, $attempts, $isolationLevel) {
            $lastError = null;

            for ($attempt = 1; $attempt <= $attempts; $attempt++) {
                $tx = null;

                try {
                    /** @var Transaction $tx */
                    $tx = await($this->beginTransaction($isolationLevel));

                    $result = await(async(fn () => $callback($tx)));

                    await($tx->commit());

                    return $result;
                } catch (\Throwable $e) {
                    $lastError = $e;

                    if ($tx !== null && $tx->isActive()) {
                        try {
                            await($tx->rollback());
                        } catch (\Throwable $rollbackError) {
                            // Continue to retry logic
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
     * @return PromiseInterface<array<string, int>>
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
     * @return array<string, int|bool>
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
     * Clears the prepared statement cache for all connections.
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
     * @return void
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Gets a prepared statement from cache or creates and caches a new one.
     *
     * @param Connection $conn
     * @param string $sql
     * @return PromiseInterface<PreparedStatement>
     */
    private function getCachedStatement(Connection $conn, string $sql): PromiseInterface
    {
        if (! isset($this->statementCaches[$conn])) {
            $this->statementCaches[$conn] = new ArrayCache($this->statementCacheSize);
        }

        $cache = $this->statementCaches[$conn];

        return $cache->get($sql)
            ->then(function ($stmt) use ($conn, $sql, $cache) {
                if ($stmt instanceof PreparedStatement) {
                    return Promise::resolved($stmt);
                }

                return $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($sql, $cache) {
                        $cache->set($sql, $stmt);

                        return $stmt;
                    })
                ;
            })
        ;
    }

    /**
     * Gets the connection pool instance.
     *
     * @return PoolManager
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    private function getPool(): PoolManager
    {
        if (! $this->isInitialized || $this->pool === null) {
            throw new NotInitializedException(
                'MysqlClient instance has not been initialized or has been close.'
            );
        }

        return $this->pool;
    }
}
