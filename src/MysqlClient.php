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
 *
 * ## Query Cancellation
 *
 * By default, cancelling a query promise dispatches KILL QUERY to the server
 * via a dedicated side-channel TCP connection. This stops the server-side query
 * immediately, releases locks, and returns the connection to the pool as fast
 * as possible.
 *
 * This behaviour can be disabled by passing `enableServerSideCancellation: false`.
 * When disabled, cancelling a promise only transitions the promise to the
 * cancelled state — the server-side query runs to completion, the connection
 * remains unavailable until it finishes, and no side-channel connection is
 * opened.
 *
 * Disable query cancellation when:
 *   - A proxy or load balancer may route the kill connection to a different node.
 *   - Connection quotas make side-channel connections unacceptable.
 *   - Query duration is already bounded by server-side timeouts
 *     (e.g. SET SESSION max_execution_time).
 *   - You prefer predictable connection-count behaviour over fast cancellation.
 *
 * Note: disabling query cancellation does not affect promise semantics —
 * $promise->cancel() still works and the promise still transitions to the
 * cancelled state immediately. Only the server-side kill is suppressed.
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

    private bool $enableStatementCache;

    private bool $resetConnectionEnabled = false;

    /**
     * Creates a new independent MysqlClient instance.
     *
     * Each instance manages its own connection pool and is completely
     * independent from other instances, allowing true multi-database support.
     *
     * @param ConnectionParams|array<string, mixed>|string $config Database configuration.
     * @param int  $maxConnections       Maximum number of connections in the pool.
     * @param int  $idleTimeout          Seconds a connection can remain idle before being closed.
     * @param int  $maxLifetime          Maximum seconds a connection can live before being rotated.
     * @param int  $statementCacheSize   Maximum number of prepared statements to cache per connection.
     * @param bool $enableStatementCache Whether to enable prepared statement caching. Defaults to true.
     * @param bool $enableServerSideCancellation Whether to dispatch KILL QUERY to the server when a
     *                                      query promise is cancelled. Defaults to true.
     * @param bool $resetConnection      Whether to issue COM_RESET_CONNECTION before returning
     *                                      connections to the pool. Clears all session state, variables,
     *                                      and prepared statements. Defaults to false.
     *
     * @throws ConfigurationException If configuration is invalid.
     */
    public function __construct(
        ConnectionParams|array|string $config,
        int $maxConnections = 10,
        int $idleTimeout = 60,
        int $maxLifetime = 3600,
        int $statementCacheSize = 256,
        bool $enableStatementCache = true,
        bool $enableServerSideCancellation = true,
        bool $resetConnection = false,
    ) {
        try {
            // Ensure the resetConnection parameter is passed along if the config is an array or string
            if (\is_array($config)) {
                $config['reset_connection'] ??= $resetConnection;
            } elseif (\is_string($config)) {
                $separator = str_contains($config, '?') ? '&' : '?';
                if (! str_contains($config, 'reset_connection=')) {
                    $config .= $separator . 'reset_connection=' . ($resetConnection ? 'true' : 'false');
                }
            }

            $this->pool = new PoolManager(
                $config,
                $maxConnections,
                $idleTimeout,
                $maxLifetime,
                enableServerSideCancellation: $enableServerSideCancellation,
            );

            // Cache the resolved setting from the pool to avoid array lookups on every query
            $this->resetConnectionEnabled = (bool) ($this->pool->getStats()['reset_connection_enabled'] ?? false);
            $this->statementCacheSize = $statementCacheSize;
            $this->enableStatementCache = $enableStatementCache;

            if ($this->enableStatementCache) {
                /** @var \WeakMap<Connection, ArrayCache> $map */
                $map = new \WeakMap();
                $this->statementCaches = $map;
            }
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
        $pool = $this->getPool();
        $connection = null;

        return $this->withCancellation(
            $this->borrowConnection()
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
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<MysqlResult>
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $this->withCancellation(
            $this->borrowConnection()
                ->then(function (Connection $conn) use ($sql, $params, &$connection) {
                    $connection = $conn;

                    if (\count($params) === 0) {
                        return $conn->query($sql);
                    }

                    if ($this->enableStatementCache) {
                        return $this->getCachedStatement($conn, $sql)
                            ->then(function (PreparedStatement $stmt) use ($params) {
                                return $stmt->execute(array_values($params));
                            })
                        ;
                    }

                    /** @var PreparedStatement|null $stmtRef */
                    $stmtRef = null;

                    return $conn->prepare($sql)
                        ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                            $stmtRef = $stmt;

                            return $stmt->execute(array_values($params));
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
     *
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<int>
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->getAffectedRows())
        );
    }

    /**
     * {@inheritdoc}
     *
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<int>
     */
    public function executeGetId(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->getLastInsertId())
        );
    }

    /**
     * {@inheritdoc}
     *
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<array<string, mixed>|null>
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->fetchOne())
        );
    }

    /**
     * {@inheritdoc}
     *
     * @param array<int|string, mixed> $params
     * @return PromiseInterface<mixed>
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
     * @param string                   $sql        SQL query to stream.
     * @param array<int|string, mixed> $params     Query parameters (optional).
     * @param int                      $bufferSize Maximum rows to buffer before applying backpressure.
     * @return PromiseInterface<MysqlRowStream>
     */
    public function stream(string $sql, array $params = [], int $bufferSize = 100): PromiseInterface
    {
        $pool = $this->getPool();

        $state = new class () {
            public ?Connection $connection = null;
            public bool $released = false;
        };

        $releaseOnce = function () use ($pool, $state): void {
            if ($state->released || $state->connection === null) {
                return;
            }
            $state->released = true;
            $pool->release($state->connection);
        };

        return $this->withCancellation(
            $this->borrowConnection()
                ->then(function (Connection $conn) use ($sql, $params, $bufferSize, $pool, $state) {
                    $state->connection = $conn;

                    if (\count($params) === 0) {
                        $innerStreamPromise = $conn->streamQuery($sql, $bufferSize);
                    } else {
                        $innerStreamPromise = $this->getCachedStatement($conn, $sql)
                            ->then(function (PreparedStatement $stmt) use ($params, $bufferSize) {
                                return $stmt->executeStream(array_values($params), $bufferSize);
                            })
                        ;
                    }

                    $q = $innerStreamPromise->then(
                        function (MysqlRowStream $stream) use ($conn, $pool, $state): MysqlRowStream {
                            if ($stream instanceof Internals\RowStream) {
                                $state->released = true;

                                $stream->waitForCommand()->finally(function () use ($pool, $conn): void {
                                    $pool->release($conn);
                                });
                            } else {
                                $state->released = true;
                                $pool->release($conn);
                            }

                            return $stream;
                        },
                        function (\Throwable $e) use ($conn, $pool, $state): never {
                            if (! $state->released) {
                                $state->released = true;
                                $pool->release($conn);
                            }

                            throw $e;
                        }
                    );

                    $q->onCancel(static function () use ($innerStreamPromise): void {
                        if (! $innerStreamPromise->isSettled()) {
                            $innerStreamPromise->cancelChain();
                        }
                    });

                    return $q;
                })
                ->finally($releaseOnce)
        );
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<TransactionInterface>
     */
    public function beginTransaction(?IsolationLevelInterface $isolationLevel = null): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;

        return $this->withCancellation(
            $this->borrowConnection()
                ->then(function (Connection $conn) use ($isolationLevel, $pool, &$connection) {
                    $connection = $conn;

                    // Get the cache for this specific connection, if any
                    $cache = $this->getCacheForConnection($conn);

                    $promise = $isolationLevel !== null
                        ? $conn->query("SET TRANSACTION ISOLATION LEVEL {$isolationLevel->toSql()}")
                        ->then(fn () => $conn->query('START TRANSACTION'))
                        : $conn->query('START TRANSACTION');

                    return $promise->then(function () use ($conn, $pool, $cache) {
                        // Pass the cache to the Transaction
                        return new Transaction($conn, $pool, $cache);
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
     * @param callable(TransactionInterface): mixed $callback
     * @param int                                   $attempts Number of retry attempts (default: 1).
     * @return PromiseInterface<mixed>
     *
     * @throws \InvalidArgumentException If attempts is less than 1.
     * @throws \Throwable                The final exception if all attempts fail.
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

                    $result = await(async(fn () => $callback($tx)));

                    await($tx->commit());

                    return $result;
                } catch (\Throwable $e) {
                    $lastError = $e;

                    if ($tx !== null && $tx->isActive()) {
                        try {
                            await($tx->rollback());
                        } catch (\Throwable) {
                            // Continue to retry logic.
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
     * @return PromiseInterface<array<string, int>>
     * @throws NotInitializedException If this instance is not initialized.
     */
    public function healthCheck(): PromiseInterface
    {
        return $this->getPool()->healthCheck();
    }

    /**
     * {@inheritdoc}
     *
     * @return array<string, bool|int>
     * @throws NotInitializedException If this instance is not initialized.
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
        $clientStats['statement_cache_size'] = $this->statementCacheSize;

        return $clientStats;
    }

    /**
     * Clears the prepared statement cache for all connections.
     */
    public function clearStatementCache(): void
    {
        if ($this->statementCaches !== null) {
            /** @var \WeakMap<Connection, ArrayCache> $map */
            $map = new \WeakMap();
            $this->statementCaches = $map;
        }
    }

    /**
     * Closes the client, releasing all pooled connections and resources.
     */
    public function close(): void
    {
        if ($this->pool === null) {
            return;
        }

        $this->pool->close();
        $this->pool = null;
        $this->statementCaches = null;
    }

    /**
     * Destructor ensures cleanup on object destruction.
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Borrows a connection from the pool and handles cache invalidation.
     *
     * If COM_RESET_CONNECTION is enabled, the server automatically drops all
     * prepared statements upon the connection being returned to the pool. We
     * must clear the local statement cache for this specific connection upon
     * checkout to ensure we don't attempt to execute a dropped statement ID.
     *
     * @return PromiseInterface<Connection>
     */
    private function borrowConnection(): PromiseInterface
    {
        $pool = $this->getPool();

        return $pool->get()->then(function (Connection $conn) {
            if ($this->resetConnectionEnabled && $this->statementCaches !== null) {
                $this->statementCaches->offsetUnset($conn);
            }

            return $conn;
        });
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
     * Helper to retrieve or create the statement cache for a specific connection.
     * Returns null if caching is disabled.
     */
    private function getCacheForConnection(Connection $conn): ?ArrayCache
    {
        if (! $this->enableStatementCache || $this->statementCaches === null) {
            return null;
        }

        if (! $this->statementCaches->offsetExists($conn)) {
            $this->statementCaches->offsetSet($conn, new ArrayCache($this->statementCacheSize));
        }

        return $this->statementCaches->offsetGet($conn);
    }

    /**
     * Gets a prepared statement from cache or prepares and caches a new one.
     *
     * @param Connection $conn
     * @param string $sql
     * @return PromiseInterface<PreparedStatement>
     */
    private function getCachedStatement(Connection $conn, string $sql): PromiseInterface
    {
        $cache = $this->getCacheForConnection($conn);

        if ($cache === null) {
            return $conn->prepare($sql);
        }

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
     * @throws NotInitializedException If the client has not been initialized or has been closed.
     */
    private function getPool(): PoolManager
    {
        if ($this->pool === null) {
            throw new NotInitializedException(
                'MysqlClient instance has not been initialized or has been closed.'
            );
        }

        return $this->pool;
    }
}
