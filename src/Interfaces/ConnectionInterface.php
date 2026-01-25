<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\ValueObjects\Result;
use Hibla\Promise\Interfaces\PromiseInterface;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;

/**
 * Interface for MySQL database connections.
 *
 * Defines the contract for interacting with a MySQL server,
 * including connection management, query execution, and state tracking.
 */
interface ConnectionInterface
{
    /**
     * Establish connection to the MySQL server.
     *
     * @return PromiseInterface<ConnectionInterface> Resolves with the connection instance
     */
    public function connect(): PromiseInterface;

    /**
     * Execute a SQL query that returns rows.
     *
     * @param string $sql The SQL query to execute
     * @return PromiseInterface<Result> Resolves with query results
     */
    public function query(string $sql): PromiseInterface;

    /**
     * Execute a SQL command that doesn't return rows (INSERT, UPDATE, DELETE, etc.).
     *
     * @param string $sql The SQL command to execute
     * @return PromiseInterface<OkPacket> Resolves with execution metadata
     */
    public function execute(string $sql): PromiseInterface;

    /**
     * Ping the server to check if connection is alive.
     *
     * @return PromiseInterface<bool> Resolves with true if server responds
     */
    public function ping(): PromiseInterface;

    /**
     * Close the connection gracefully.
     *
     * @return void
     */
    public function close(): void;

    /**
     * Get the current connection state.
     *
     * @return ConnectionState
     */
    public function getState(): ConnectionState;

    /**
     * Check if the connection is ready to execute queries.
     *
     * @return bool
     */
    public function isReady(): bool;

    /**
     * Check if the connection is closed.
     *
     * @return bool
     */
    public function isClosed(): bool;
}
