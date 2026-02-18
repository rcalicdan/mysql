<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Sql\Exceptions\TransactionException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\Result as ResultInterface;
use Hibla\Sql\Transaction as TransactionInterface;

/**
 * Transaction implementation with automatic pool management.
 *
 * @internal Created by MysqlClient::beginTransaction() - do not instantiate directly.
 */
class Transaction implements TransactionInterface
{
    /**
     * @var list<callable(): void>
     */
    private array $onCommitCallbacks = [];

    /**
     * @var list<callable(): void>
     */
    private array $onRollbackCallbacks = [];

    private bool $active = true;

    private bool $released = false;

    /**
     * @internal Use MysqlClient::beginTransaction() instead.
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly PoolManager $pool
    ) {}

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
        $this->ensureActive();

        if (\count($params) === 0) {
            return $this->withCancellation($this->connection->query($sql));
        }

        /** @var PreparedStatement|null $stmtRef */
        $stmtRef = null;

        return $this->withCancellation(
            $this->connection->prepare($sql)
                ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                    $stmtRef = $stmt;

                    return $stmt->execute($params);
                })
                ->finally(function () use (&$stmtRef): void {
                    if ($stmtRef !== null) {
                        $stmtRef->close();
                    }
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
        $this->ensureActive();

        if (\count($params) === 0) {
            return $this->withCancellation(
                $this->connection->streamQuery($sql, $bufferSize)
            );
        }

        return $this->withCancellation(
            $this->connection->prepare($sql)
                ->then(function (PreparedStatement $stmt) use ($params, $bufferSize): PromiseInterface {
                    return $stmt->executeStream(array_values($params), $bufferSize)
                        ->then(function (MysqlRowStream $stream) use ($stmt): MysqlRowStream {
                            if ($stream instanceof RowStream) {
        
                                $stream->waitForCommand()->finally($stmt->close(...));
                            }

                            return $stream;
                        })
                    ;
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
        $this->ensureActive();
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
        $this->ensureActive();

        return $this->connection->prepare($sql);
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
            ->finally(function (): void {
                $this->releaseConnection();
            })
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
            ->finally(function (): void {
                $this->releaseConnection();
            })
        ;
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function savepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("SAVEPOINT {$escaped}")
            ->then(
                function (): void {},
                function (\Throwable $e) use ($identifier): never {
                    throw new TransactionException(
                        "Failed to create savepoint '{$identifier}': " . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
        ;
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

        return $this->connection->query("ROLLBACK TO SAVEPOINT {$escaped}")
            ->then(
                function (): void {},
                function (\Throwable $e) use ($identifier): never {
                    throw new TransactionException(
                        "Failed to rollback to savepoint '{$identifier}': " . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
        ;
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("RELEASE SAVEPOINT {$escaped}")
            ->then(
                function (): void {},
                function (\Throwable $e) use ($identifier): never {
                    throw new TransactionException(
                        "Failed to release savepoint '{$identifier}': " . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
        ;
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
     * Transaction query methods return the LEAF of a promise chain. When a
     * user calls cancel() on that leaf, it only cancels that node and its
     * children — it never reaches the ROOT where the real onCancel handler
     * (KILL QUERY dispatch) is registered on the Connection.
     *
     * This bridge registers an onCancel hook so that cancel() on the leaf
     * immediately walks up to the root via cancelChain(), triggering KILL
     * QUERY correctly without requiring the caller to use cancelChain().
     *
     * NOTE: This is intentionally NOT applied to commit() and rollback() —
     * those must never be interrupted via KILL QUERY.
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

    private function releaseConnection(): void
    {
        if ($this->released) {
            return;
        }

        $this->onCommitCallbacks   = [];
        $this->onRollbackCallbacks = [];
        $this->released            = true;
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
     * Destructor ensures the connection is released when the transaction
     * goes out of scope without explicit commit/rollback.
     */
    public function __destruct()
    {
        $this->releaseConnection();
    }
}