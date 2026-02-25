<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

/**
 * Statistics about a completed stream operation.
 *
 * Provides metadata about the streaming query execution,
 * including row count, column count, and duration.
 *
 * @package Hibla\Mysql\ValueObjects
 */
class StreamStats
{
    public function __construct(
        public readonly int $rowCount,
        public readonly int $columnCount,
        public readonly float $duration,
        public readonly int $warningCount = 0,
        public readonly int $connectionId = 0
    ) {
    }

    /**
     * Get the average rows per second.
     */
    public function getRowsPerSecond(): float
    {
        return $this->duration > 0 ? $this->rowCount / $this->duration : 0.0;
    }

    /**
     * Check if any rows were streamed.
     */
    public function hasRows(): bool
    {
        return $this->rowCount > 0;
    }
}
