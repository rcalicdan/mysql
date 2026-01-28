<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\ValueObjects;

/**
 * Statistics about a completed stream operation.
 *
 * Provides metadata about the streaming query execution,
 * including row count, column count, and duration.
 *
 * @package Hibla\MysqlClient\ValueObjects
 */
final class StreamStats
{
    /**
     * @param int $rowCount Total number of rows streamed
     * @param int $columnCount Number of columns in the result set
     * @param float $duration Duration of the stream operation in seconds
     */
    public function __construct(
        public readonly int $rowCount,
        public readonly int $columnCount,
        public readonly float $duration,
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