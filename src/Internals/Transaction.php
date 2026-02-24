<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Cache\ArrayCache;
use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\TransactionException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\Result as ResultInterface;
use Hibla\Sql\Transaction as TransactionInterface;

/**
 * Transaction implementation with automatic pool management and state protection.
 *
 * @internal Created by MysqlClient::beginTransaction() - do not instantiate directly.
 */
class Transaction implements TransactionInterface
{
    /** @var list<callable(): void> */
    private array $onCommitCallbacks = [];

    /** @var list<callable(): void> */
    private array $onRollbackCallbacks = [];

    private bool $active = true;
    private bool $released = false;

    /**
     * If a query fails mid-transaction, the transaction becomes "tainted".
     * MySQL may allow further queries, but it is dangerous and can lead to
     * partial commits depending on the storage engine. We enforce a strict failure state.
     */
    private bool $failed = false;

    /**
     * @internal Use MysqlClient::beginTransaction() instead.
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly PoolManager $pool,
        private readonly ?ArrayCache $statementCache = null
    ) {
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
        $this->ensureActiveAndNotFailed();

        if (\count($params) === 0) {
            $promise = $this->connection->query($sql);
        } else {
            $promise = $this->getCachedStatement($sql)
                ->then(function (array $result) use ($params) {
                    /** @var PreparedStatement $stmt */
                    [$stmt, $isCached] = $result;

                    return $stmt->execute($params)
                        ->finally(function () use ($stmt, $isCached) {
                            if (!$isCached) {
                                $stmt->close();
                            }
                        });
                });
        }

        return $this->withCancellation($this->trackErrorState($promise));
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
        $this->ensureActiveAndNotFailed();

        if (\count($params) === 0) {
            $promise = $this->connection->streamQuery($sql, $bufferSize);
        } else {
            $promise = $this->getCachedStatement($sql)
                ->then(function (array $result) use ($params, $bufferSize) {
                    /** @var PreparedStatement $stmt */
                    [$stmt, $isCached] = $result;

                    return $stmt->executeStream(array_values($params), $bufferSize)
                        ->then(function (MysqlRowStream $stream) use ($stmt, $isCached): MysqlRowStream {
                            if ($stream instanceof RowStream) {
                                if (!$isCached) {
                                    $stream->waitForCommand()->finally($stmt->close(...));
                                }
                            }

                            return $stream;
                        });
                });
        }

        return $this->withCancellation($this->trackErrorState($promise));
    }

    /**
     * {@inheritdoc}
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
     */
    public function onCommit(callable $callback): void
    {
        $this->ensureActive(); // Callbacks can be added even if tainted, before rollback
        $this->onCommitCallbacks[] = $callback;
    }

    /**
     * {@inheritdoc}
     */
    public function onRollback(callable $callback): void
    {
        $this->ensureActive();
        $this->onRollbackCallbacks[] = $callback;
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PreparedStatementInterface>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();

        $promise = $this->connection->prepare($sql)->then(
            function (PreparedStatementInterface $stmt) {
                return new TransactionPreparedStatement($stmt, $this->connection);
            }
        );

        return $this->trackErrorState($promise);
    }

    /**
     * {@inheritdoc}
     *
     * NOTE: withCancellation() is intentionally NOT applied to commit().
     * Dispatching KILL QUERY against a COMMIT would leave the transaction
     * in an undefined state on the server. This operation must be allowed
     * to complete or fail on its own terms.
     *
     * @return PromiseInterface<void>
     */
    public function commit(): PromiseInterface
    {
        $this->ensureActive();

        if ($this->failed) {
            return Promise::rejected(
                new TransactionException('Cannot commit: Transaction was aborted due to a previous query error. You must explicitly rollback().')
            );
        }

        $this->active = false;

        return $this->connection->query('COMMIT')
            ->then(
                function (): void {
                    $this->executeCallbacks($this->onCommitCallbacks);
                    $this->onRollbackCallbacks = [];
                },
                function (\Throwable $e): never {
                    throw new TransactionException(
                        'Failed to commit transaction: ' . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * {@inheritdoc}
     *
     * NOTE: withCancellation() is intentionally NOT applied to rollback().
     * Dispatching KILL QUERY against a ROLLBACK would leave the transaction
     * in an undefined state on the server. This operation must be allowed
     * to complete or fail on its own terms.
     *
     * @return PromiseInterface<void>
     */
    public function rollback(): PromiseInterface
    {
        $this->ensureActive(); 
        $this->active = false;
        $this->failed = false;

        return $this->connection->query('ROLLBACK')
            ->then(
                function (): void {
                    $this->executeCallbacks($this->onRollbackCallbacks);
                    $this->onCommitCallbacks = [];
                },
                function (\Throwable $e): never {
                    throw new TransactionException(
                        'Failed to rollback transaction: ' . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function savepoint(string $identifier): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();
        $escaped = $this->escapeIdentifier($identifier);

        $promise = $this->connection->query("SAVEPOINT {$escaped}")->catch(
            function (\Throwable $e) use ($identifier): never {
                throw new TransactionException(
                    "Failed to create savepoint '{$identifier}': " . $e->getMessage(),
                    (int) $e->getCode(),
                    $e
                );
            }
        );

        return $this->trackErrorState($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function rollbackTo(string $identifier): PromiseInterface
    {
        $this->ensureActive(); 
        $escaped = $this->escapeIdentifier($identifier);

        // Rolling back to a savepoint potentially clears the failed state for operations after that savepoint
        $this->failed = false;

        return $this->connection->query("ROLLBACK TO SAVEPOINT {$escaped}")->catch(
            function (\Throwable $e) use ($identifier): never {
                $this->failed = true; // If rollback fails, the whole transaction is dead
                throw new TransactionException(
                    "Failed to rollback to savepoint '{$identifier}': " . $e->getMessage(),
                    (int) $e->getCode(),
                    $e
                );
            }
        );
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();
        $escaped = $this->escapeIdentifier($identifier);

        $promise = $this->connection->query("RELEASE SAVEPOINT {$escaped}")->catch(
            function (\Throwable $e) use ($identifier): never {
                throw new TransactionException(
                    "Failed to release savepoint '{$identifier}': " . $e->getMessage(),
                    (int) $e->getCode(),
                    $e
                );
            }
        );

        return $this->trackErrorState($promise);
    }

    /**
     * {@inheritdoc}
     */
    public function isActive(): bool
    {
        return $this->active && ! $this->connection->isClosed();
    }

    /**
     * {@inheritdoc}
     */
    public function isClosed(): bool
    {
        return $this->connection->isClosed();
    }

    /**
     * Bridges cancel() → cancelChain() on a public-facing promise.
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
     * Tracks the state of the promise. If it rejects, the transaction is marked as failed.
     *
     * @template T
     * @param PromiseInterface<T> $promise
     * @return PromiseInterface<T>
     */
    private function trackErrorState(PromiseInterface $promise): PromiseInterface
    {
        return $promise->catch(function (\Throwable $e) {
            $this->failed = true;
            throw $e;
        });
    }

    /**
     * Helper to get a prepared statement from cache or create a new one.
     * Returns an array [PreparedStatement, bool isCached].
     *
     * @return PromiseInterface<array{0: PreparedStatement, 1: bool}>
     */
    private function getCachedStatement(string $sql): PromiseInterface
    {
        if ($this->statementCache === null) {
            return $this->connection->prepare($sql)->then(fn ($stmt) => [$stmt, false]);
        }

        return $this->statementCache->get($sql)->then(function (mixed $stmt) use ($sql) {
            if ($stmt instanceof PreparedStatement) {
                return [$stmt, true];
            }

            return $this->connection->prepare($sql)->then(function (PreparedStatement $newStmt) use ($sql) {
                $this->statementCache->set($sql, $newStmt);

                return [$newStmt, true];
            });
        });
    }

    private function releaseConnection(): void
    {
        if ($this->released) {
            return;
        }

        $this->onCommitCallbacks = [];
        $this->onRollbackCallbacks = [];
        $this->released = true;
        $this->pool->release($this->connection);
    }

    /**
     * @param list<callable(): void> $callbacks
     */
    private function executeCallbacks(array $callbacks): void
    {
        foreach ($callbacks as $callback) {
            $callback();
        }
    }

    private function ensureActive(): void
    {
        if ($this->connection->isClosed()) {
            throw new TransactionException('Cannot perform operation: connection is closed');
        }

        if (! $this->active) {
            throw new TransactionException('Cannot perform operation: transaction is no longer active');
        }
    }

    private function ensureActiveAndNotFailed(): void
    {
        $this->ensureActive();

        if ($this->failed) {
            throw new TransactionException('Transaction aborted due to a previous query error. You must explicitly rollback().');
        }
    }

    private function escapeIdentifier(string $identifier): string
    {
        if ($identifier === '') {
            throw new \InvalidArgumentException('Savepoint identifier cannot be empty');
        }

        if (\strlen($identifier) > 64) {
            throw new \InvalidArgumentException('Savepoint identifier too long (max 64 characters)');
        }

        if (strpos($identifier, "\0") !== false || strpos($identifier, "\xFF") !== false) {
            throw new \InvalidArgumentException('Savepoint identifier contains invalid byte values');
        }

        if ($identifier !== trim($identifier)) {
            throw new \InvalidArgumentException('Savepoint identifier cannot start or end with spaces');
        }

        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    /**
     * Destructor ensures the connection is released safely.
     *
     * If the transaction was not explicitly committed or rolled back (e.g. an exception
     * was thrown and the variable went out of scope), it issue an asynchronous
     * fire-and-forget ROLLBACK to clear the session state before returning the
     * connection to the pool.
     */
    public function __destruct()
    {
        if ($this->active && ! $this->connection->isClosed() && ! $this->released) {
            $this->active = false;

            $this->connection->query('ROLLBACK')->finally($this->releaseConnection(...));
        } elseif (! $this->released) {
            $this->releaseConnection();
        }
    }
}