<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use function Hibla\await;

use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use SplQueue;
use Throwable;

/**
 * Provides an asynchronous stream of rows using PHP Generators.
 *
 * @internal This must not be used directly.
 */
class RowStream implements MysqlRowStream
{
    /**
     * Default maximum number of rows to buffer before applying backpressure.
     */
    private const int DEFAULT_BUFFER_SIZE = 100;

    /**
     * @var SplQueue<array<string, mixed>>
     */
    private SplQueue $buffer;

    /**
     * @var Promise<array<string, mixed>|null>|null
     */
    private ?Promise $waiter = null;

    /**
     * @var Promise<void>
     */
    private Promise $commandPromise;

    /** @var PromiseInterface<mixed>|null */
    private ?PromiseInterface $commandQueuePromise = null;

    /**
     * @var \Closure(bool): void|null
     */
    private ?\Closure $onBackpressure = null;

    private ?StreamStats $stats = null;

    private ?Throwable $error = null;

    private bool $completed = false;

    private bool $cancelled = false;

    private int $maxBufferSize;

    private int $resumeThreshold;

    /**
     * @param int $bufferSize Maximum number of rows to buffer (default: 100)
     */
    public function __construct(int $bufferSize = self::DEFAULT_BUFFER_SIZE)
    {
        if ($bufferSize < 1) {
            throw new \InvalidArgumentException('Buffer size must be at least 1');
        }

        $this->maxBufferSize = $bufferSize;
        $this->resumeThreshold = (int) ($bufferSize / 2);

        /** @var SplQueue<array<string, mixed>> $buffer */
        $buffer = new SplQueue();
        $this->buffer = $buffer;

        /** @var Promise<void> $commandPromise */
        $commandPromise = new Promise();
        $this->commandPromise = $commandPromise;
    }

    /**
     * @return \Generator<int, array<string, mixed>>
     */
    public function getIterator(): \Generator
    {
        while (true) {
            if ($this->error !== null) {
                throw $this->error;
            }

            if (! $this->buffer->isEmpty()) {
                $row = $this->buffer->dequeue();

                // Resume socket reading when buffer drains below threshold
                if ($this->onBackpressure !== null && $this->buffer->count() < $this->resumeThreshold) {
                    ($this->onBackpressure)(false);
                }

                yield $row;

                continue;
            }

            if ($this->completed) {
                break;
            }

            /** @var Promise<array<string, mixed>|null> $waiter */
            $waiter = new Promise();
            $this->waiter = $waiter;

            /** @var array<string, mixed>|null $row */
            $row = await($waiter);

            if ($row === null) {
                break;
            }

            yield $row;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getStats(): ?StreamStats
    {
        return $this->stats;
    }

    /**
     * Cancels the stream (Phase 2 cancellation — after the stream promise has resolved).
     *
     * Always sets the cancelled flag regardless of the completed state, so
     * isCancelled() reliably reflects caller intent even when called after a
     * fast query that already buffered all rows and called complete().
     *
     * If the command is still running (stream not yet completed), it also:
     *  - Propagates cancel() to the command queue promise → KILL QUERY dispatch
     *  - Rejects the internal waiter promise → unblocks a suspended foreach
     *
     * Calling cancel() more than once is a no-op.
     */
    public function cancel(): void
    {
        if ($this->cancelled) {
            return;
        }

        $this->cancelled = true;
        // Always set the error so getIterator()'s error check fires on the
        // next iteration regardless of whether complete() already ran.
        $this->error = new CancelledException('Stream was cancelled');

        if (! $this->completed) {
            $this->completed = true;

            if (
                $this->commandQueuePromise !== null
                && ! $this->commandQueuePromise->isSettled()
                && ! $this->commandQueuePromise->isCancelled()
            ) {
                $this->commandQueuePromise->cancel();
            }

            if ($this->waiter !== null) {
                $waiter = $this->waiter;
                $this->waiter = null;
                $waiter->reject($this->error);
            }
        }

        // Discard any buffered rows so they are not yielded after cancellation.
        // This covers the case where complete() already ran but the caller
        // cancels mid-iteration before draining the full buffer.
        while (! $this->buffer->isEmpty()) {
            $this->buffer->dequeue();
        }
    }

    /**
     * Returns whether this stream has been cancelled.
     */
    public function isCancelled(): bool
    {
        return $this->cancelled;
    }

    /**
     * Stores the command queue promise so cancel() can reach it after
     * the outer streamQuery promise has already fulfilled.
     *
     * Called by Connection::streamQuery() immediately after enqueueCommand().
     * @param PromiseInterface<mixed> $promise
     * @internal
     */
    public function setCommandPromise(PromiseInterface $promise): void
    {
        $this->commandQueuePromise = $promise;
    }

    /**
     * Sets the backpressure handler for controlling socket flow.
     *
     * @internal
     * @param \Closure(bool): void $handler Callback receiving true to pause, false to resume
     */
    public function setBackpressureHandler(\Closure $handler): void
    {
        $this->onBackpressure = $handler;
    }

    /**
     * Pushes a row into the stream buffer.
     *
     * @internal
     * @param array<string, mixed> $row
     */
    public function push(array $row): void
    {
        if ($this->cancelled) {
            return;
        }

        if ($this->waiter !== null) {
            $promise = $this->waiter;
            $this->waiter = null;
            $promise->resolve($row);
        } else {
            $this->buffer->enqueue($row);

            // Apply backpressure when buffer grows too large
            if ($this->onBackpressure !== null && $this->buffer->count() >= $this->maxBufferSize) {
                ($this->onBackpressure)(true);
            }
        }
    }

    /**
     * Marks the stream as complete with final statistics.
     *
     * @internal
     */
    public function complete(StreamStats $stats): void
    {
        if ($this->cancelled) {
            return;
        }

        $this->stats = $stats;
        $this->completed = true;

        if ($this->waiter !== null) {
            $promise = $this->waiter;
            $this->waiter = null;
            $promise->resolve(null);
        }
    }

    /**
     * Marks the stream as failed with an error.
     *
     * @internal
     */
    public function error(Throwable $e): void
    {
        if ($this->cancelled) {
            return;
        }

        $this->error = $e;
        $this->completed = true;

        if ($this->waiter !== null) {
            $promise = $this->waiter;
            $this->waiter = null;
            $promise->reject($e);
        }

        if ($this->commandPromise->isPending()) {
            $this->commandPromise->reject($e);
        }
    }

    /**
     * Called by Connection when the command is fully finished and state is READY.
     *
     * @internal
     */
    public function markCommandFinished(): void
    {
        if ($this->commandPromise->isPending()) {
            $this->commandPromise->resolve(null);
        }
    }

    /**
     * Returns a promise that resolves when the underlying database command is fully complete
     * and the connection is ready to be reused.
     *
     * @internal
     * @return Promise<void>
     */
    public function waitForCommand(): Promise
    {
        return $this->commandPromise;
    }
}
