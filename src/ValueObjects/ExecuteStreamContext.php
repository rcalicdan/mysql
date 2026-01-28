<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\ValueObjects;

use Hibla\MysqlClient\Internals\PreparedStatement;

/**
 * Context for streaming prepared statement execution.
 * 
 * @internal
 * @package Hibla\MysqlClient\ValueObjects
 */
final readonly class ExecuteStreamContext
{
    public function __construct(
        public PreparedStatement $statement,
        public StreamContext $streamContext
    ) {
    }
}