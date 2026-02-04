<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Transaction implementation with automatic pool management.
 *
 * This class is exclusively created by MysqlClient and always manages
 * pooled connections. The connection is automatically released back to
 * the pool on commit/rollback or when the transaction goes out of scope.
 *
 * @internal Created by MysqlClient::beginTransaction() - do not instantiate directly.
 */
class Transaction
{
    private bool $active = true;
    private bool $released = false;

    /** @var list<callable(): void> */
    private array $onCommitCallbacks = [];

    /** @var list<callable(): void> */
    private array $onRollbackCallbacks = [];

    /**
     * Creates a new transaction with automatic pool management.
     *
     * @internal Use MysqlClient::beginTransaction() instead.
     *
     * @param Connection $connection The database connection
     * @param PoolManager $pool The pool manager for auto-release
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly PoolManager $pool
    ) {
    }

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

                return $stmt->execute($params);
            })
            ->finally(function () use (&$stmtRef) {
                if ($stmtRef !== null) {
                    return $stmtRef->close();
                }
            })
        ;
    }

    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params);
    }

    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->query($sql, $params)
            ->then(fn (Result $result) => $result->fetchOne())
        ;
    }

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

    public function onCommit(callable $callback): void
    {
        $this->ensureActive();
        $this->onCommitCallbacks[] = $callback;
    }

    public function onRollback(callable $callback): void
    {
        $this->ensureActive();
        $this->onRollbackCallbacks[] = $callback;
    }

    public function prepare(string $sql): PromiseInterface
    {
        $this->ensureActive();

        return $this->connection->prepare($sql);
    }

    public function commit(): PromiseInterface
    {
        $this->ensureActive();
        $this->active = false;

        return $this->connection->query('COMMIT')
            ->then(function () {
                $this->executeCallbacks($this->onCommitCallbacks);
                $this->onRollbackCallbacks = [];

                return null;
            })
            ->finally(function () {
                $this->releaseConnection();
            })
        ;
    }

    public function rollback(): PromiseInterface
    {
        $this->ensureActive();
        $this->active = false;

        return $this->connection->query('ROLLBACK')
            ->then(function () {
                $this->executeCallbacks($this->onRollbackCallbacks);
                $this->onCommitCallbacks = [];

                return null;
            })
            ->finally(function () {
                $this->releaseConnection();
            })
        ;
    }

    public function savepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("SAVEPOINT {$escaped}")
            ->then(fn () => null)
        ;
    }

    public function rollbackTo(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("ROLLBACK TO SAVEPOINT {$escaped}")
            ->then(fn () => null)
        ;
    }

    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("RELEASE SAVEPOINT {$escaped}")
            ->then(fn () => null)
        ;
    }

    public function isActive(): bool
    {
        return $this->active && ! $this->connection->isClosed();
    }

    public function isClosed(): bool
    {
        return $this->connection->isClosed();
    }

    /**
     * Destructor ensures the connection is released when the transaction
     * goes out of scope without explicit commit/rollback.
     */
    public function __destruct()
    {
        $this->releaseConnection();
    }

    /**
     * Releases the connection back to the pool.
     * This is automatically called on commit/rollback.
     *
     * @return void
     */
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

        if (! $this->active) {
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
