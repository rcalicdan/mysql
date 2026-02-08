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

    public function __construct()
    {
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
                yield $this->buffer->dequeue();

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
        }
    }

    /**
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
     * @internal Called by Connection when the command is fully finished and state is READY.
     */
    public function markCommandFinished(): void
    {
        if ($this->commandPromise->isPending()) {
            $this->commandPromise->resolve(null);
        }
    }

    /**
     * @internal
     *
     * Returns a promise that resolves when the underlying database command is fully complete
     * and the connection is ready to be reused.
     *
     * @return Promise<void>
     */
    public function waitForCommand(): Promise
    {
        return $this->commandPromise;
    }
}