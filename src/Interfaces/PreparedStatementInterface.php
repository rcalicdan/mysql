<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Interface for prepared statements.
 *
 * Prepared statements provide a way to execute the same SQL query
 * multiple times with different parameters efficiently and securely.
 *
 * They prevent SQL injection and improve performance for repeated queries.
 */
interface PreparedStatementInterface
{
    /**
     * Execute the prepared statement with the given parameters.
     *
     * The return type depends on the statement type:
     * - SELECT queries return QueryResult with rows
     * - INSERT/UPDATE/DELETE return ExecuteResult with metadata
     *
     * @param array<int|string, mixed> $params Parameters to bind to the statement
     * @return PromiseInterface<QueryResult|ExecuteResult> Resolves with execution results
     */
    public function execute(array $params = []): PromiseInterface;

    /**
     * Close the prepared statement and free resources on the server.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface;

    /**
     * Get the statement ID assigned by the server.
     *
     * @return int
     */
    public function getStatementId(): int;

    /**
     * Get the number of parameters in the statement.
     *
     * @return int
     */
    public function getParameterCount(): int;

    /**
     * Get the number of columns in the result set (for SELECT statements).
     * Returns 0 for non-SELECT statements.
     *
     * @return int
     */
    public function getColumnCount(): int;

    /**
     * Check if this is a SELECT statement that returns rows.
     *
     * @return bool
     */
    public function isQuery(): bool;
}
