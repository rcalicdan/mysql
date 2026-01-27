<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\Enums\TransactionIsolationLevel;
use Hibla\MysqlClient\PreparedStatement;
use Hibla\MysqlClient\Transaction;
use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\Promise\Interfaces\PromiseInterface;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;

/**
 * Main MySQL Client interface.
 *
 * This is the primary entry point for interacting with MySQL.
 * It provides high-level methods for database operations.
 */
interface ClientInterface
{
    /**
     * Execute a SQL query that returns rows.
     *
     * @param string $sql The SQL query to execute
     * @return PromiseInterface<QueryResult> Resolves with query results
     */
    public function query(string $sql): PromiseInterface;

    /**
     * Execute a SQL command that doesn't return rows.
     *
     * @param string $sql The SQL command to execute
     * @return PromiseInterface<ExecuteResult> Resolves with execution metadata
     */
    public function execute(string $sql): PromiseInterface;

    /**
     * Prepare a SQL statement for repeated execution.
     *
     * @param string $sql The SQL statement with ? placeholders
     * @return PromiseInterface<PreparedStatement> Resolves with prepared statement
     */
    public function prepare(string $sql): PromiseInterface;

    /**
     * Begin a transaction.
     *
     * @param TransactionIsolationLevel|null $isolationLevel Optional isolation level for this transaction
     * @return PromiseInterface<Transaction>
     */
    public function beginTransaction(?TransactionIsolationLevel $isolationLevel = null): PromiseInterface;

    /**
     * Commit the current transaction.
     *
     * @return PromiseInterface<OkPacket>
     */
    public function commit(): PromiseInterface;

    /**
     * Rollback the current transaction.
     *
     * @return PromiseInterface<OkPacket>
     */
    public function rollback(): PromiseInterface;

    /**
     * Ping the server to check if connection is alive.
     *
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface;

    /**
     * Close the client and all underlying connections.
     *
     * @return void
     */
    public function close(): void;

    /**
     * Get the underlying connection instance.
     *
     * @return ConnectionInterface
     */
    public function getConnection(): ConnectionInterface;
}
