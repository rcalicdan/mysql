<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\ValueObjects\Result;
use Hibla\Promise\Interfaces\PromiseInterface;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;

/**
 * Interface for prepared statements.
 *
 * Prepared statements provide a way to execute the same SQL query
 * multiple times with different parameters efficiently.
 */
interface PreparedStatementInterface
{
    /**
     * Execute the prepared statement with the given parameters.
     *
     * @param array<int, mixed> $params Parameters to bind to the statement
     * @return PromiseInterface<Result|OkPacket> Resolves with execution results
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
     *
     * @return int
     */
    public function getColumnCount(): int;
}
