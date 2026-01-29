<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Internals\Connection as MysqlConnection;
use Hibla\Promise\Interfaces\PromiseInterface;

class Transaction
{
    private bool $active = true;

    public function __construct(
        private readonly MysqlConnection $connection
    ) {
    }

    /**
     * Executes a query within the transaction.
     * @param string $sql
     * @return PromiseInterface<QueryResult>
     */
    public function query(string $sql): PromiseInterface
    {
        $this->ensureActive();

        return $this->connection->query($sql);
    }

    /**
     * Executes a statement within the transaction.
     * @param string $sql
     * @return PromiseInterface<ExecuteResult>
     */
    public function execute(string $sql): PromiseInterface
    {
        $this->ensureActive();

        return $this->connection->execute($sql);
    }

    /**
     * Prepares a statement for execution within the transaction.
     * @param string $sql
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $this->ensureActive();

        return $this->connection->prepare($sql);
    }

    /**
     * Commits the transaction, making all changes permanent.
     * @return PromiseInterface<void>
     */
    public function commit(): PromiseInterface
    {
        $this->ensureActive();
        $this->active = false;

        return $this->connection->query('COMMIT')
            ->then(fn () => null)
        ;
    }

    /**
     * Rolls back the transaction, discarding all changes.
     * @return PromiseInterface<void>
     */
    public function rollback(): PromiseInterface
    {
        $this->ensureActive();
        $this->active = false;

        return $this->connection->query('ROLLBACK')
            ->then(fn () => null)
        ;
    }

    /**
     * Creates a savepoint within the transaction.
     * @param string $identifier The name of the savepoint.
     * @return PromiseInterface<void>
     */
    public function savepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("SAVEPOINT {$escaped}")
            ->then(fn () => null)
        ;
    }

    /**
     * Rolls back the transaction to a named savepoint.
     * @param string $identifier The name of the savepoint to roll back to.
     * @return PromiseInterface<void>
     */
    public function rollbackTo(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("ROLLBACK TO SAVEPOINT {$escaped}")
            ->then(fn () => null)
        ;
    }

    /**
     * Releases a named savepoint.
     * @param string $identifier The name of the savepoint to release.
     * @return PromiseInterface<void>
     */
    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        return $this->connection->query("RELEASE SAVEPOINT {$escaped}")
            ->then(fn () => null)
        ;
    }

    /**
     * Checks if the transaction is still active.
     * It becomes inactive after a commit or rollback.
     */
    public function isActive(): bool
    {
        return $this->active && ! $this->connection->isClosed();
    }

    /**
     * Checks if the parent connection has been closed.
     */
    public function isClosed(): bool
    {
        return $this->connection->isClosed();
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
