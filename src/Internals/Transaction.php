<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Internals\Connection as MysqlConnection;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Represents a database transaction with automatic connection pool management.
 *
 * This class provides a unified API for transaction operations, including:
 * - Transaction control (commit, rollback, savepoints)
 * - Query execution (with and without parameters)
 * - Convenience methods (fetchOne, fetchValue)
 * - Prepared statements
 * - Automatic connection release back to the pool
 */
class Transaction
{
    private bool $active = true;
    private bool $released = false;

    /** @var list<callable(): void> */
    private array $onCommitCallbacks = [];

    /** @var list<callable(): void> */
    private array $onRollbackCallbacks = [];

    public function __construct(
        private readonly MysqlConnection $connection,
        private readonly ?PoolManager $pool = null
    ) {}

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * If parameters are provided, the query is automatically prepared as a
     * prepared statement before execution to prevent SQL injection.
     *
     * @param string $sql SQL query to execute with optional ? placeholders
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<QueryResult>
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        $this->ensureActive();

        if (\count($params) === 0) {
            return $this->connection->query($sql);
        }

        /** @var PreparedStatement|null $stmtRef */
        $stmtRef = null;

        return $this->connection->prepare($sql)
            ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                $stmtRef = $stmt;
                return $stmt->executeStatement($params);
            })
            ->finally(function () use (&$stmtRef) {
                if ($stmtRef !== null) {
                    return $stmtRef->close();
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
     * @return PromiseInterface<ExecuteResult>
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        $this->ensureActive();

        if (\count($params) === 0) {
            return $this->connection->execute($sql);
        }

        /** @var PreparedStatement|null $stmtRef */
        $stmtRef = null;

        return $this->connection->prepare($sql)
            ->then(function (PreparedStatement $stmt) use ($params, &$stmtRef) {
                $stmtRef = $stmt;
                return $stmt->executeStatement($params);
            })
            ->finally(function () use (&$stmtRef) {
                if ($stmtRef !== null) {
                    return $stmtRef->close();
                }
            });
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * Returns null if no rows match the query.
     *
     * @param string $sql SQL query to execute with optional ? placeholders
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<array<string, mixed>|null>
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(function ($result) {
                return $result->fetchOne();
            });
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * Useful for queries that return a single scalar value like COUNT, MAX, etc.
     * Returns null if the query returns no rows.
     *
     * @param string $sql SQL query to execute with optional ? placeholders
     * @param string|int $column Column name or index (default: 0)
     * @param array<int, mixed> $params Optional parameters for prepared statement
     * @return PromiseInterface<mixed>
     */
    public function fetchValue(string $sql, string|int $column = 0, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(function ($result) use ($column) {
                $row = $result->fetchOne();
                if ($row === null) {
                    return null;
                }

                return $row[$column] ?? null;
            });
    }

    /**
     * Registers a callback to be executed only if the transaction is successfully committed.
     *
     * Callbacks are executed after the COMMIT command succeeds but before the main
     * commit() promise is resolved. If a callback throws an exception, the commit()
     * promise will be rejected with that exception.
     *
     * @param callable(): void $callback The closure to execute on commit.
     */
    public function onCommit(callable $callback): void
    {
        $this->ensureActive();
        $this->onCommitCallbacks[] = $callback;
    }

    /**
     * Registers a callback to be executed only if the transaction is rolled back.
     *
     * Callbacks are executed after the ROLLBACK command succeeds but before the main
     * rollback() promise is resolved. If a callback throws an exception, the
     * rollback() promise will be rejected with that exception.
     *
     * @param callable(): void $callback The closure to execute on rollback.
     */
    public function onRollback(callable $callback): void
    {
        $this->ensureActive();
        $this->onRollbackCallbacks[] = $callback;
    }


    /**
     * Prepares a statement for execution within the transaction.
     *
     * @param string $sql SQL statement with ? placeholders
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $this->ensureActive();

        return $this->connection->prepare($sql);
    }

    /**
     * Commits the transaction, making all changes permanent.
     * Automatically releases the connection back to the pool if pooled.
     *
     * @return PromiseInterface<void>
     */
    public function commit(): PromiseInterface // MODIFIED
    {
        $this->ensureActive();
        $this->active = false;

        return $this->connection->query('COMMIT')
            ->then(function () {
                // On successful commit, execute the registered callbacks.
                $this->executeCallbacks($this->onCommitCallbacks);
                $this->onRollbackCallbacks = []; // Discard rollback callbacks
                return null;
            })
            ->finally(function () {
                $this->releaseConnection();
            });
    }

    /**
     * Rolls back the transaction, discarding all changes.
     * Automatically releases the connection back to the pool if pooled.
     *
     * @return PromiseInterface<void>
     */
    public function rollback(): PromiseInterface // MODIFIED
    {
        $this->ensureActive();
        $this->active = false;

        return $this->connection->query('ROLLBACK')
            ->then(function () {
                // On successful rollback, execute the registered callbacks.
                $this->executeCallbacks($this->onRollbackCallbacks);
                $this->onCommitCallbacks = []; // Discard commit callbacks
                return null;
            })
            ->finally(function () {
                $this->releaseConnection();
            });
    }

    /**
     * Creates a savepoint within the transaction.
     *
     * @param string $identifier The name of the savepoint.
     * @return PromiseInterface<void>
     */
    public function savepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("SAVEPOINT {$escaped}")
            ->then(fn() => null);
    }

    /**
     * Rolls back the transaction to a named savepoint.
     *
     * @param string $identifier The name of the savepoint to roll back to.
     * @return PromiseInterface<void>
     */
    public function rollbackTo(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("ROLLBACK TO SAVEPOINT {$escaped}")
            ->then(fn() => null);
    }

    /**
     * Releases a named savepoint.
     *
     * @param string $identifier The name of the savepoint to release.
     * @return PromiseInterface<void>
     */
    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("RELEASE SAVEPOINT {$escaped}")
            ->then(fn() => null);
    }

    /**
     * Checks if the transaction is still active.
     * It becomes inactive after a commit or rollback.
     */
    public function isActive(): bool
    {
        return $this->active && !$this->connection->isClosed();
    }

    /**
     * Checks if the parent connection has been closed.
     */
    public function isClosed(): bool
    {
        return $this->connection->isClosed();
    }

    /**
     * Releases the connection back to the pool.
     * This is called automatically by commit() and rollback() if the transaction is pooled.
     */
    private function releaseConnection(): void
    {
        if ($this->released || $this->pool === null) {
            return;
        }

        $this->released = true;
        $this->pool->release($this->connection);
    }

    /**
     * Destructor ensures the connection is released if the transaction
     * object is destroyed without explicit commit/rollback.
     */
    public function __destruct()
    {
        $this->releaseConnection();
    }

    /**
     * Executes an array of callbacks, failing the promise chain on the first error.
     *
     * @param list<callable(): void> $callbacks
     * @throws \Throwable
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
            throw new \RuntimeException('Connection is closed');
        }

        if (!$this->active) {
            throw new \LogicException('Transaction is no longer active');
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
}
