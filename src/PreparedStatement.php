<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

class PreparedStatement
{
    /**
     * @param array<int, ColumnDefinition> $columnDefinitions
     * @param array<int, ColumnDefinition> $paramDefinitions
     */
    public function __construct(
        private readonly MysqlConnection $connection,
        public readonly int $id,
        public readonly int $numColumns,
        public readonly int $numParams,
        public readonly array $columnDefinitions = [],
        public readonly array $paramDefinitions = []
    ) {}

    /**
     * Execute the prepared statement with the given parameters.
     *
     * @param array<int, mixed> $params The parameters to bind to the statement
     * @return PromiseInterface<ExecuteResult|QueryResult>
     */
    public function execute(array $params = []): PromiseInterface
    {
        if (\count($params) !== $this->numParams) {
            throw new \InvalidArgumentException(
                \sprintf("Statement expects %d parameters, got %d", $this->numParams, count($params))
            );
        }

        $normalizedParams = $this->normalizeParameters($params);

        return $this->connection->executeStatement($this, $normalizedParams);
    }

    /**
     * Normalize parameters to ensure proper type handling in MySQL binary protocol.
     * 
     * The binary protocol requires specific type conversions:
     * - Booleans must be sent as integers (1 or 0) since MySQL BOOLEAN is TINYINT(1)
     * - This prevents the CommandBuilder from misinterpreting booleans as strings
     *
     * @param array<int, mixed> $params Raw parameters from user
     * @return array<int, mixed> Normalized parameters ready for binary protocol
     */
    private function normalizeParameters(array $params): array
    {
        $normalized = [];
        
        foreach ($params as $index => $value) {
            if (\is_bool($value)) {
                $normalized[$index] = $value ? 1 : 0;
            }
            else {
                $normalized[$index] = $value;
            }
        }
        
        return $normalized;
    }

    /**
     * Close the prepared statement and free server resources.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        return $this->connection->closeStatement($this->id);
    }
}