<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\PreparedStatement;
use Hibla\MysqlClient\Internals\ExecuteResult;
use Hibla\MysqlClient\Internals\QueryResult;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Represents an active MySQL transaction, providing methods to execute
 * commands within the transaction and to control its lifecycle.
 */
interface TransactionInterface
{
    /**
     * Executes a query within the transaction.
     * @param string $sql
     * @return PromiseInterface<QueryResult|ExecuteResult>
     */
    public function query(string $sql): PromiseInterface;

    /**
     * Executes a statement within the transaction.
     * @param string $sql
     * @return PromiseInterface<QueryResult|ExecuteResult>
     */
    public function execute(string $sql): PromiseInterface;

    /**
     * Prepares a statement for execution within the transaction.
     * @param string $sql
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface;

    /**
     * Commits the transaction, making all changes permanent.
     * @return PromiseInterface<ExecuteResult>
     */
    public function commit(): PromiseInterface;

    /**
     * Rolls back the transaction, discarding all changes.
     * @return PromiseInterface<ExecuteResult>
     */
    public function rollback(): PromiseInterface;

    /**
     * Creates a savepoint within the transaction.
     * @param string $identifier The name of the savepoint.
     * @return PromiseInterface<ExecuteResult>
     */
    public function savepoint(string $identifier): PromiseInterface;

    /**
     * Rolls back the transaction to a named savepoint.
     * @param string $identifier The name of the savepoint to roll back to.
     * @return PromiseInterface<ExecuteResult>
     */
    public function rollbackTo(string $identifier): PromiseInterface;

    /**
     * Releases a named savepoint.
     * @param string $identifier The name of the savepoint to release.
     * @return PromiseInterface<ExecuteResult>
     */
    public function releaseSavepoint(string $identifier): PromiseInterface;

    /**
     * Checks if the transaction is still active.
     * It becomes inactive after a commit or rollback.
     */
    public function isActive(): bool;

    /**
     * Checks if the parent connection has been closed.
     */
    public function isClosed(): bool;
}
