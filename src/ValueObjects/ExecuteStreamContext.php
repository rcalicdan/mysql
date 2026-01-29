<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

use Hibla\Mysql\Internals\PreparedStatement;

/**
 * Context for streaming prepared statement execution.
 *
 * @internal
 * @package Hibla\Mysql\ValueObjects
 */
final readonly class ExecuteStreamContext
{
    public function __construct(
        public PreparedStatement $statement,
        public StreamContext $streamContext
    ) {
    }
}
