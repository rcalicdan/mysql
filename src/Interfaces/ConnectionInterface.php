<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Interface for MySQL database connections.
 */
interface ConnectionInterface
{
    /**
     * Establish connection to the MySQL server.
     *
     * @return PromiseInterface<ConnectionInterface>
     */
    public function connect(): PromiseInterface;

    /**
     * Execute a SELECT query that returns rows.
     *
     * @param string $sql The SQL SELECT query to execute
     * @return PromiseInterface<QueryResult>
     */
    public function query(string $sql): PromiseInterface;

    /**
     * Execute a write operation (INSERT, UPDATE, DELETE, etc.).
     *
     * @param string $sql The SQL command to execute
     * @return PromiseInterface<ExecuteResult>
     */
    public function execute(string $sql): PromiseInterface;

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