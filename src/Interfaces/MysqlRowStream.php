<?php

declare(strict_types=1);

namespace Hibla\Mysql\Interfaces;

use Hibla\Mysql\ValueObjects\StreamStats;
use IteratorAggregate;

/**
 * Represents a streaming MySQL result set.
 *
 * @extends IteratorAggregate<int, array<string, mixed>>
 */
interface MysqlRowStream extends IteratorAggregate
{
    /**
     * Returns statistics about the completed stream, or null if still in progress.
     *
     * @return StreamStats|null
     */
    public function getStats(): ?StreamStats;

    /**
     * Cancels the stream and releases resources.
     *
     * Phase 1 (pre-resolution): If called before await($streamPromise) returns,
     * the underlying command promise is cancelled and KILL QUERY is dispatched
     * to MySQL. The await() call will throw CancelledException.
     *
     * Phase 2 (mid-iteration): If called during foreach iteration after the
     * promise has already resolved, the stream is marked cancelled, buffered
     * rows are discarded, and the next iteration throws CancelledException.
     * KILL QUERY is only dispatched if the command is still in-flight.
     *
     * Calling cancel() multiple times is safe (idempotent no-op).
     *
     * @return void
     */
    public function cancel(): void;

    /**
     * Returns whether this stream has been cancelled.
     *
     * @return bool
     */
    public function isCancelled(): bool;
}