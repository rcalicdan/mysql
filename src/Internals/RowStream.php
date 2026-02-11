<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use function Hibla\await;

use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\ValueObjects\StreamStats;
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
    private const DEFAULT_BUFFER_SIZE = 100;

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

    private ?StreamStats $stats = null;

    private ?Throwable $error = null;

    private bool $completed = false;

    /**
     * Callback for backpressure control (pause/resume socket reading).
     * 
     * @var \Closure(bool): void|null
     */
    private ?\Closure $onBackpressure = null;

    /**
     * Maximum number of rows to buffer before pausing socket reading.
     */
    private int $maxBufferSize;

    /**
     * Threshold at which to resume socket reading (typically half of max).
     */
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
        $this->resumeThreshold = (int)($bufferSize / 2);

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

            if (!$this->buffer->isEmpty()) {
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