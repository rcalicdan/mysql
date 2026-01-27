<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

class PreparedStatement
{
    private bool $isClosed = false;

    /**
     * @param MysqlConnection $connection The parent connection object.
     * @param int $id The server-side statement ID.
     * @param int $numColumns Number of columns in the result set.
     * @param int $numParams Number of parameters the statement expects.
     * @param array<int, ColumnDefinition> $columnDefinitions Metadata for result set columns.
     * @param array<int, ColumnDefinition> $paramDefinitions Metadata for parameters.
     */
    public function __construct(
        private readonly MysqlConnection $connection,
        public readonly int $id,
        public readonly int $numColumns,
        public readonly int $numParams,
        public readonly array $columnDefinitions = [],
        public readonly array $paramDefinitions = []
    ) {
        register_shutdown_function([$this, 'close']);
    }

    /**
     * Execute the prepared statement with the given parameters.
     *
     * @param array<int, mixed> $params The parameters to bind to the statement.
     * @return PromiseInterface<ExecuteResult|QueryResult>
     */
    public function executeStatement(array $params = []): PromiseInterface
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
