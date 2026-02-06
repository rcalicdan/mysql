<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use IteratorAggregate;
use SplQueue;
use Throwable;

use function Hibla\await;

/**
 * Provides an asynchronous stream of rows using PHP Generators.
 * 
 * This must not be use diretly.
 */
class RowStream implements IteratorAggregate
{
    /** @var SplQueue<array|null> Buffer for rows */
    private SplQueue $buffer;

    /** @var Promise|null Pending promise waiting for data */
    private ?Promise $waiter = null;

    /** @var Promise Promise that resolves when the underlying connection command is finished */
    private Promise $commandPromise;

    private ?StreamStats $stats = null;
    private ?Throwable $error = null;
    private bool $completed = false;

    public function __construct()
    {
        $this->buffer = new SplQueue();
        $this->commandPromise = new Promise();
    }

    /**
     * @internal
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

            $this->waiter = new Promise();
            $row = await($this->waiter);

            if ($row === null) {
                break;
            }

            yield $row;
        }
    }

    public function getStats(): ?StreamStats
    {
        return $this->stats;
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
     * Returns a promise that resolves when the underlying database command is fully complete
     * and the connection is ready to be reused.
     */
    public function waitForCommand(): Promise
    {
        return $this->commandPromise;
    }
}