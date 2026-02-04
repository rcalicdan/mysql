<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

/**
 * Represents a prepared SQL statement that can be executed multiple times
 * with different parameter values.
 *
 * Prepared statements provide:
 * - Protection against SQL injection
 * - Improved performance for repeated queries
 * - Support for both buffered and streaming execution
 *
 * @package Hibla\Mysql\Internals
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
    ) {
    }

    /**
     * Execute the prepared statement with the given parameters (buffered).
     *
     * All result rows are loaded into memory. Use executeStream() for large result sets.
     *
     * @param array<int, mixed> $params The parameters to bind to the statement.
     * @return PromiseInterface<Result>
     * @throws \RuntimeException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function execute(array $params = []): PromiseInterface
    {
        if ($this->isClosed) {
            throw new \RuntimeException('Cannot execute a closed statement.');
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
     * Rows are delivered via callback as they arrive, without buffering in memory.
     * Ideal for processing large result sets (millions of rows).
     *
     * Example:
     * ```php
     * $stmt = await($connection->prepare('SELECT * FROM logs WHERE user_id = ?'));
     * $stats = await($stmt->executeStream(
     *     params: [$userId],
     *     onRow: function (array $row) {
     *         writeToFile($row);
     *     }
     * ));
     * echo "Exported {$stats->rowCount} rows\n";
     * ```
     *
     * @param array<int, mixed> $params The parameters to bind to the statement.
     * @param callable(array): void $onRow Callback invoked for each row.
     * @param callable(StreamStats): void|null $onComplete Optional callback when streaming completes.
     * @param callable(\Throwable): void|null $onError Optional callback for error handling.
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
        if ($this->isClosed) {
            throw new \RuntimeException('Cannot execute a closed statement.');
        }

        if (\count($params) !== $this->numParams) {
            throw new \InvalidArgumentException(
                \sprintf('Statement expects %d parameters, got %d', $this->numParams, \count($params))
            );
        }

        $normalizedParams = $this->normalizeParameters($params);
        $context = new StreamContext($onRow, $onComplete, $onError);

        return $this->connection->executeStream($this, $normalizedParams, $context);
    }

    /**
     * Closes the prepared statement and deallocates it on the server.
     * This method is idempotent and safe to call multiple times.
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
     *
     * @param array $params
     * @return array
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
