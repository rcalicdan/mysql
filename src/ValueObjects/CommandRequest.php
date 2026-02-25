<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

use Hibla\Promise\Promise;

/**
 * Represents a queued command to be executed on the MySQL connection.
 *
 * @internal
 * @package Hibla\Mysql\ValueObjects
 */
final class CommandRequest
{
    public const string TYPE_QUERY = 'query';
    public const string TYPE_EXECUTE = 'execute';
    public const string TYPE_PING = 'ping';
    public const string TYPE_PREPARE = 'prepare';
    public const string TYPE_CLOSE_STMT = 'close_stmt';
    public const string TYPE_STREAM_QUERY = 'stream_query';
    public const string TYPE_EXECUTE_STREAM = 'execute_stream';
    public const string TYPE_RESET = 'reset';

    /**
     * @param string $type The type of command (one of the TYPE_* constants)
     * @param Promise<mixed> $promise The promise to resolve/reject when command completes
     * @param string $sql The SQL query string (for query/prepare/stream commands)
     * @param array<mixed> $params Parameters for prepared statement execution
     * @param int $statementId The statement ID (for execute/close commands)
     * @param mixed $context Additional context data (e.g., StreamContext for streaming)
     */
    public function __construct(
        public readonly string $type,
        public readonly Promise $promise,
        public readonly string $sql = '',
        public readonly array $params = [],
        public readonly int $statementId = 0,
        public readonly mixed $context = null,
    ) {
    }
}
