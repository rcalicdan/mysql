<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use function Hibla\async;

use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\PreparedException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;

use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

/**
 * Represents a prepared SQL statement that can be executed multiple times
 * with different parameter values.
 *
 * This class must not be instantiated directly.
 */
class PreparedStatement implements PreparedStatementInterface
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
     * {@inheritdoc}
     *
     * @return PromiseInterface<MysqlResult>
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
     * {@inheritdoc}
     *
     * @return PromiseInterface<MysqlRowStream>
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

            /** @var MysqlRowStream $stream */
            return $stream;
        });
    }

    /**
     * {@inheritdoc}
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
     * @param array<int, mixed> $params
     * @return array<int, mixed>
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
