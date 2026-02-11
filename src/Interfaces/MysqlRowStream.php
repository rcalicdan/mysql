<?php

declare(strict_types=1);

namespace Hibla\Mysql\Interfaces;

use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Sql\RowStream as RowStreamInterface;

/**
 * Provides an asynchronous stream of rows with access to MySQL-specific StreamStats.
 *
 * This interface should be used when stream metadata is required.
 */
interface MysqlRowStream extends RowStreamInterface
{
    /**
     * Gets the statistics about the completed stream operation.
     *
     * The stats object is guaranteed to be available after the generator
     * returned by getIterator() is exhausted.
     *
     * @return StreamStats|null
     */
    public function getStats(): ?StreamStats;
}
