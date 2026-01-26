<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\ValueObjects;

use Hibla\Promise\Promise;

final readonly class CommandRequest
{
    public const TYPE_QUERY = 'QUERY';
    public const TYPE_PING = 'PING';
    public const TYPE_PREPARE = 'PREPARE';
    public const TYPE_EXECUTE = 'EXECUTE';
    public const TYPE_CLOSE_STMT = 'CLOSE_STMT';

    public function __construct(
        public string $type,
        public Promise $promise,
        public string $sql = '',
        public array $params = [],
        public int $statementId = 0,
        public mixed $context = null // Holds the PreparedStatement object during Execute
    ) {
    }
}