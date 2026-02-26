<?php

declare(strict_types=1);

namespace Hibla\Mysql\Interfaces;

use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * Provides a limited query surface for onConnect hooks.
 *
 * Intentionally minimal — exposes only what is needed to initialize session
 * state (SET SESSION, USE, etc.) on a fresh connection. The underlying
 * connection object is internal and never leaked.
 *
 * Both query() and execute() are provided so that hooks can inspect result
 * metadata if needed (e.g. confirm a SET succeeded or read a server variable).
 */
interface ConnectionSetup
{
    /**
     * Runs a SQL query and resolves with the full result.
     * Use this when you need to inspect the result (rows, affected rows, etc.).
     *
     * @return PromiseInterface<MysqlResult>
     */
    public function query(string $sql): PromiseInterface;

    /**
     * Runs a SQL statement and resolves with the number of affected rows.
     * Convenience wrapper for fire-and-forget SET SESSION statements.
     *
     * @return PromiseInterface<int>
     */
    public function execute(string $sql): PromiseInterface;
}