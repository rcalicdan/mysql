<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use function Hibla\async;

use Hibla\Mysql\Interfaces\MysqlResult;

use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\PreparedException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Stream\Traits\PromiseHelperTrait;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

/**
 * Represents a prepared SQL statement that can be executed multiple times.
 *
 * @internal
 */
class PreparedStatement implements PreparedStatementInterface
{
    use PromiseHelperTrait;

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
     * {@inheritdoc}
     *
     * @param array<int, mixed> $params
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
     * @param array<int, mixed> $params
     * @param int $bufferSize Maximum rows to buffer before applying backpressure (default: 100)
     * @return PromiseInterface<MysqlRowStream>
     */
    public function executeStream(array $params = [], int $bufferSize = 100): PromiseInterface
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
        $stream           = new RowStream($bufferSize);

        $stream->setBackpressureHandler(function (bool $shouldPause): void {
            $shouldPause ? $this->connection->pause() : $this->connection->resume();
        });

        /** @var Promise<MysqlRowStream> $outerPromise */
        $outerPromise = new Promise();

        $context = new StreamContext(
            onRow: function (array $row) use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->resolve($stream);
                }
                $stream->push($row);
            },
            onComplete: function (StreamStats $stats) use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->resolve($stream);
                }
                $stream->complete($stats);
            },
            onError: function (\Throwable $e) use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->reject($e);
                }
                $stream->error($e);
            }
        );

        /** @var PromiseInterface<StreamStats> $commandPromise */
        $commandPromise = $this->connection->executeStream($this, $normalizedParams, $context);

        $stream->setCommandPromise($commandPromise);

        $commandPromise->then(
            $stream->markCommandFinished(...),
            $stream->error(...)
        );

        $outerPromise->onCancel(function () use ($stream): void {
            $stream->cancel();
        });

        return $outerPromise;
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            return $this->createResolvedVoidPromise();
        }

        $this->isClosed = true;

        /** @var PromiseInterface<void> $promise */
        $promise = $this->connection->closeStatement($this->id);

        return $promise;
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
