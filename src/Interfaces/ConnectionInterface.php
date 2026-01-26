<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\PreparedStatement;
use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Interface for MySQL database connections.
 */
interface ConnectionInterface
{
    /**
     * Establish connection to the MySQL server.
     *
     * @return PromiseInterface<self>
     */
    public function connect(): PromiseInterface;

    /**
     * @param string $sql The SQL query to execute
     * @return PromiseInterface<QueryResult>
     */
    public function query(string $sql): PromiseInterface;

    /**
     * Execute a SQL statement.
     * 
     * @param string $sql The SQL command to execute
     * @return PromiseInterface<ExecuteResult>
     */
    public function execute(string $sql): PromiseInterface;

    /**
     * Prepare a SQL statement for execution.
     *
     * @param string $sql The SQL statement with placeholders (?)
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface;

    /**
     * Ping the server to check if connection is alive.
     *
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface;

    /**
     * Close the connection gracefully.
     */
    public function close(): void;

    /**
     * Get the current connection state.
     */
    public function getState(): ConnectionState;

    /**
     * Check if the connection is ready to execute queries.
     */
    public function isReady(): bool;

    /**
     * Check if the connection is closed.
     */
    public function isClosed(): bool;
}