<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\PreparedException;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

use function Hibla\async;

/**
 * Represents a prepared SQL statement that can be executed multiple times
 * with different parameter values.
 */
class PreparedStatement
{
    private bool $isClosed = false;

    /**
     * @param Connection $connection The parent connection object.
     * @param int $id The server-side statement ID.
     * @param int $numColumns Number of columns in the result set.
     * @param int $numParams Number of parameters the statement expects.
     * @param array<int, ColumnDefinition> $columnDefinitions Metadata for result set columns.
     * @param array<int, ColumnDefinition> $paramDefinitions Metadata for parameters.
     */
    public function __construct(
        private readonly Connection $connection,
        public readonly int $id,
        public readonly int $numColumns,
        public readonly int $numParams,
        public readonly array $columnDefinitions = [],
        public readonly array $paramDefinitions = []
    ) {}

    /**
     * Execute the prepared statement with the given parameters (buffered).
     *
     * @param array<int, mixed> $params The parameters to bind to the statement.
     * @return PromiseInterface<Result>
     */
    public function execute(array $params = []): PromiseInterface
    {
        if ($this->isClosed) {
            throw new PreparedException('Cannot execute a closed statement.');
        }

        if (\count($params) !== $this->numParams) {
            throw new \InvalidArgumentException(
                \sprintf('Statement expects %d parameters, got %d', $this->numParams, \count($params))
            );
        }

        $normalizedParams = $this->normalizeParameters($params);

        return $this->connection->executeStatement($this, $normalizedParams);
    }

    /**
     * Execute the prepared statement with streaming (memory-efficient).
     *
     * Returns a Promise that resolves to an RowStream.
     *
     * @param array<int, mixed> $params The parameters to bind to the statement.
     * @return PromiseInterface<RowStream>
     * @throws PreparedException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function executeStream(array $params = []): PromiseInterface
    {
        if ($this->isClosed) {
            throw new PreparedException('Cannot execute a closed statement.');
        }

        if (\count($params) !== $this->numParams) {
            throw new \InvalidArgumentException(
                \sprintf('Statement expects %d parameters, got %d', $this->numParams, \count($params))
            );
        }

        return async(function () use ($params) {
            $stream = new RowStream();

            $context = new StreamContext(
                onRow: $stream->push(...),
                onComplete: $stream->complete(...),
                onError: $stream->error(...)
            );

            $normalizedParams = $this->normalizeParameters($params);

            $commandPromise = $this->connection->executeStream($this, $normalizedParams, $context);

            $commandPromise->then(
                $stream->markCommandFinished(...),
                $stream->error(...)
            );

            return $stream;
        });
    }

    /**
     * Closes the prepared statement and deallocates it on the server.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::resolved();
        }

        $this->isClosed = true;

        return $this->connection->closeStatement($this->id);
    }

    /**
     * Normalize parameters (convert booleans to integers).
     */
    private function normalizeParameters(array $params): array
    {
        $normalized = [];
        foreach ($params as $index => $value) {
            $normalized[$index] = \is_bool($value) ? ($value ? 1 : 0) : $value;
        }

        return $normalized;
    }

    public function __destruct()
    {
        $this->close();
    }
}
