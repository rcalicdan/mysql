<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\ValueObjects;

use Hibla\Promise\Promise;

/**
 * @internal
 */
final readonly class CommandRequest
{
    public const string TYPE_QUERY = 'QUERY';
    public const string TYPE_PING = 'PING';
    
    public function __construct(
        public string $type, 
        public string $sql, 
        public Promise $promise
    ) {
    }
}