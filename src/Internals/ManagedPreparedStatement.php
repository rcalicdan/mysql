<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Manager\PoolManager;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * A wrapper around PreparedStatement that manages connection lifecycle.
 *
 * This class automatically releases the connection back to the pool when
 * the statement is closed or goes out of scope.
 */
class ManagedPreparedStatement
{
    private bool $isReleased = false;

    /**
     * @param PreparedStatement $statement The underlying prepared statement
     * @param Connection $connection The connection this statement belongs to
     * @param PoolManager $pool The pool to release the connection back to
     */
    public function __construct(
        private readonly PreparedStatement $statement,
        private readonly Connection $connection,
        private readonly PoolManager $pool
    ) {
    }

    /**
     * Execute the prepared statement with the given parameters (buffered).
     *
     * @param array<int, mixed> $params The parameters to bind to the statement
     * @return PromiseInterface<Result>
     * @throws \RuntimeException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function executeStatement(array $params = []): PromiseInterface
    {
        return $this->statement->executeStatement($params);
    }

    /**
     * Execute the prepared statement with streaming (memory-efficient).
     *
     * @param array<int, mixed> $params The parameters to bind to the statement
     * @param callable(array): void $onRow Callback invoked for each row
     * @param callable(StreamStats): void|null $onComplete Optional callback when streaming completes
     * @param callable(\Throwable): void|null $onError Optional callback for error handling
     * @return PromiseInterface<StreamStats>
     * @throws \RuntimeException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function executeStream(
        array $params,
        callable $onRow,
        ?callable $onComplete = null,
        ?callable $onError = null
    ): PromiseInterface {
        return $this->statement->executeStream($params, $onRow, $onComplete, $onError);
    }

    /**
     * Closes the prepared statement and releases the connection back to the pool.
     *
     * This method is idempotent and safe to call multiple times.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        return $this->statement->close()
            ->finally(function () {
                $this->releaseConnection();
            })
        ;
    }

    /**
     * Release the connection back to the pool.
     *
     * @return void
     */
    private function releaseConnection(): void
    {
        if ($this->isReleased) {
            return;
        }

        $this->isReleased = true;
        $this->pool->release($this->connection);
    }

    /**
     * Destructor ensures the connection is released when the statement goes out of scope.
     */
    public function __destruct()
    {
        $this->releaseConnection();
    }
}
