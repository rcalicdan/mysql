<?php

declare(strict_types=1);

namespace Hibla\Mysql;

use function Hibla\async;
use function Hibla\await;

use Hibla\Cache\ArrayCache;
use Hibla\Mysql\Exceptions\ConfigurationException;
use Hibla\Mysql\Exceptions\NotInitializedException;
use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Internals\ManagedPreparedStatement;
use Hibla\Mysql\Internals\PreparedStatement;
use Hibla\Mysql\Internals\Transaction;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\IsolationLevelInterface;
use Hibla\Sql\Result as ResultInterface;
use Hibla\Sql\SqlClientInterface;
use Hibla\Sql\Transaction as TransactionInterface;

/**
 * Instance-based Asynchronous MySQL Client with Connection Pooling.
 *
 * This class provides a high-level API for managing MySQL database connections.
 * Each instance is completely independent, allowing true multi-database support
 * without global state.
 */
final class MysqlClient implements SqlClientInterface
{
    /**
     * @var PoolManager|null
     */
    private ?PoolManager $pool = null;

    /**
     * @var \WeakMap<Connection, ArrayCache>|null
     */
    private ?\WeakMap $statementCaches = null;

    private int $statementCacheSize;

    private bool $isInitialized = false;

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
            $this->statementCacheSize  = $statementCacheSize;
            $this->enableStatementCache = $enableStatementCache;

            if ($this->enableStatementCache) {
                /** @var \WeakMap<Connection, ArrayCache> $map */
                $map                  = new \WeakMap();
                $this->statementCaches = $map;
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
     * {@inheritdoc}
     *
     * @return PromiseInterface<ManagedPreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $pool       = $this->getPool();
        $connection = null;

        return $this->withCancellation(
            $pool->get()
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
        );
    }

    /**
     * {@inheritdoc}
     *
     * - If `$params` are provided, it uses a secure PREPARED STATEMENT (Binary Protocol).
     * - If no `$params` are provided, it uses a non-prepared query (Text Protocol).
     *
     * @return PromiseInterface<MysqlResult>
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        $pool       = $this->getPool();
        $connection = null;

        return $this->withCancellation(
            $pool->get()
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
                        ->finally(function () use (&$stmtRef): void {
                            if ($stmtRef !== null) {
                                $stmtRef->close();
                            }
                        })
                    ;
                })
                ->finally(function () use ($pool, &$connection): void {
                    if ($connection !== null) {
                        $pool->release($connection);
                    }
                })
        );
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn(ResultInterface $result) => $result->getAffectedRows())
        );
    }

    /**
     * {@inheritdoc}
     */
    public function executeGetId(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn(ResultInterface $result) => $result->getLastInsertId())
        );
    }

    /**
     * {@inheritdoc}
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn(ResultInterface $result) => $result->fetchOne())
        );
    }

    /**
     * {@inheritdoc}
     */
    public function fetchValue(string $sql, string|int $column = 0, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(function (ResultInterface $result) use ($column) {
                    $row = $result->fetchOne();
                    if ($row === null) {
                        return null;
                    }

                    return $row[$column] ?? null;
                })
        );
    }

    /**
     * {@inheritdoc}
     *
     * - If `$params` are provided, it uses a secure PREPARED STATEMENT (Binary Protocol).
     * - If no `$params` are provided, it uses a non-prepared query (Text Protocol).
     *
     * @param string $sql SQL query to stream
     * @param array<int|string, mixed> $params Query parameters (optional)
     * @param int $bufferSize Maximum rows to buffer before applying backpressure (default: 100)
     * @return PromiseInterface<MysqlRowStream>
     */
    public function stream(string $sql, array $params = [], int $bufferSize = 100): PromiseInterface
    {
        $pool       = $this->getPool();
        $connection = null;
        $released   = false;

        $releaseOnce = function () use ($pool, &$connection, &$released): void {
            if ($released || $connection === null) {
                return;
            }
            $released = true;
            $pool->release($connection);
        };

        return $this->withCancellation(
            $pool->get()
                ->then(function (Connection $conn) use ($sql, $params, $bufferSize, $pool, &$connection, &$released, $releaseOnce) {
                    $connection = $conn;

                    if (\count($params) === 0) {
                        $streamPromise = $conn->streamQuery($sql, $bufferSize);
                    } else {
                        $streamPromise = $this->getCachedStatement($conn, $sql)
                            ->then(function (PreparedStatement $stmt) use ($params, $bufferSize) {
                                return $stmt->executeStream(array_values($params), $bufferSize);
                            });
                    }

                    return $streamPromise->then(
                        function (MysqlRowStream $stream) use ($conn, $pool, &$released): MysqlRowStream {
                            if ($stream instanceof Internals\RowStream) {
                                $stream->waitForCommand()->finally(function () use ($pool, $conn, &$released): void {
                                    $released = true;
                                    $pool->release($conn);
                                });
                            } else {
                                $released = true;
                                $pool->release($conn);
                            }

                            return $stream;
                        },
                        function (\Throwable $e) use ($conn, $pool, &$released): never {
                            $released = true;
                            $pool->release($conn);

                            throw $e;
                        }
                    );
                })
                ->finally($releaseOnce)
        );
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction(?IsolationLevelInterface $isolationLevel = null): PromiseInterface
    {
        $pool       = $this->getPool();
        $connection = null;

        return $this->withCancellation(
            $pool->get()
                ->then(function (Connection $conn) use ($isolationLevel, $pool, &$connection) {
                    $connection = $conn;

                    $promise = $isolationLevel !== null
                        ? $conn->query("SET TRANSACTION ISOLATION LEVEL {$isolationLevel->toSql()}")
                        ->then(fn() => $conn->query('START TRANSACTION'))
                        : $conn->query('START TRANSACTION');

                    return $promise->then(function () use ($conn, $pool) {
                        return new Transaction($conn, $pool);
                    });
                })
                ->catch(function (\Throwable $e) use ($pool, &$connection) {
                    if ($connection !== null) {
                        $pool->release($connection);
                    }

                    throw $e;
                })
        );
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable The final exception if all attempts fail
     */
    public function transaction(
        callable $callback,
        int $attempts = 1,
        ?IsolationLevelInterface $isolationLevel = null
    ): PromiseInterface {
        if ($attempts < 1) {
            throw new \InvalidArgumentException('Attempts must be at least 1');
        }

        return async(function () use ($callback, $attempts, $isolationLevel) {
            $lastError = null;

            for ($attempt = 1; $attempt <= $attempts; $attempt++) {
                $tx = null;

                try {
                    /** @var TransactionInterface $tx */
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
                            // Continue to retry logic
                        }
                    }

                    if ($attempt === $attempts) {
                        break;
                    }
                }
            }

            throw $lastError ?? new \RuntimeException('Transaction failed');
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function healthCheck(): PromiseInterface
    {
        return $this->getPool()->healthCheck();
    }

    /**
     * {@inheritdoc}
     *
     * @throws NotInitializedException If this instance is not initialized
     * @return array<string, bool|int>
     */
    public function getStats(): array
    {
        $stats = $this->getPool()->getStats();

        /** @var array<string, bool|int> $clientStats */
        $clientStats = [];
        foreach ($stats as $key => $val) {
            if (\is_bool($val) || \is_int($val)) {
                $clientStats[$key] = $val;
            }
        }

        $clientStats['statement_cache_enabled'] = $this->enableStatementCache;
        $clientStats['statement_cache_size']     = $this->statementCacheSize;

        return $clientStats;
    }

    /**
     * {@inheritdoc}
     */
    public function clearStatementCache(): void
    {
        if ($this->statementCaches !== null) {
            /** @var \WeakMap<Connection, ArrayCache> $map */
            $map                   = new \WeakMap();
            $this->statementCaches = $map;
        }
    }

    /**
     * {@inheritdoc}
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
        $this->isInitialized   = false;
    }

    /**
     * Destructor ensures cleanup on object destruction.
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Bridges cancel() → cancelChain() on a public-facing promise.
     *
     * Public methods return the LEAF of a promise chain. When a user calls
     * cancel() on that leaf, it only cancels that node and its children —
     * it never reaches the ROOT where the real onCancel handler (KILL QUERY,
     * connection release) lives.
     *
     * This bridge registers an onCancel hook so that cancel() on the leaf
     * immediately walks up to the root via cancelChain(), triggering all
     * cleanup handlers correctly — including KILL QUERY dispatch in Connection
     * and connection release back to the pool.
     *
     * @template T
     * @param PromiseInterface<T> $promise
     * @return PromiseInterface<T>
     */
    private function withCancellation(PromiseInterface $promise): PromiseInterface
    {
        $promise->onCancel($promise->cancelChain(...));

        return $promise;
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
        $caches = $this->statementCaches;
        if ($caches === null) {
            return $conn->prepare($sql);
        }

        if (! $caches->offsetExists($conn)) {
            $caches->offsetSet($conn, new ArrayCache($this->statementCacheSize));
        }

        $cache = $caches->offsetGet($conn);

        /** @var PromiseInterface<mixed> $cachePromise */
        $cachePromise = $cache->get($sql);

        return $cachePromise->then(function (mixed $stmt) use ($conn, $sql, $cache) {
            if ($stmt instanceof PreparedStatement) {
                return Promise::resolved($stmt);
            }

            return $conn->prepare($sql)
                ->then(function (PreparedStatement $stmt) use ($sql, $cache) {
                    $cache->set($sql, $stmt);

                    return $stmt;
                })
            ;
        });
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
                'MysqlClient instance has not been initialized or has been closed.'
            );
        }

        return $this->pool;
    }
}
