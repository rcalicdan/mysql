<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\ValueObjects\MysqlColumnDefinition;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

/**
 * Unified result object for all query types.
 *
 * @internal This must not be used directly.
 */
class Result implements MysqlResult
{
    private int $position = 0;

    private readonly int $numRows;

    private readonly int $columnCount;

    /**
     *  @var array<int, string>
     */
    private readonly array $columnNames;

    /**
     *  @var array<int, MysqlColumnDefinition>
     */
    private readonly array $resolvedColumns;

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, ColumnDefinition> $columnDefinitions
     */
    public function __construct(
        private readonly array $rows = [],
        private readonly int $affectedRows = 0,
        private readonly int $lastInsertId = 0,
        private readonly int $warningCount = 0,
        private readonly array $columnDefinitions = [],
        private readonly int $connectionId = 0
    ) {
        $this->numRows = \count($this->rows);

        $this->resolvedColumns = array_map(
            fn (ColumnDefinition $col) => new MysqlColumnDefinition($col),
            $this->columnDefinitions
        );

        $this->columnNames = array_map(
            fn (MysqlColumnDefinition $col) => $col->name,
            $this->resolvedColumns
        );

        $this->columnCount = \count($this->columnNames);
    }

    /**
     * {@inheritdoc}
     */
    public function getConnectionId(): int
    {
        return $this->connectionId;
    }

    /**
     * @inheritDoc
     */
    public function getFields(): array
    {
        return $this->resolvedColumns;
    }

    /**
     * {@inheritdoc}
     */
    public function getColumns(): array
    {
        return $this->columnNames;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAssoc(): ?array
    {
        if ($this->position >= $this->numRows) {
            return null;
        }

        /** @var array<string, mixed> */
        return $this->rows[$this->position++];
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll(): array
    {
        return $this->rows;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumn(string|int $column = 0): array
    {
        return array_map(fn ($row) => $row[$column] ?? null, $this->rows);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchOne(): ?array
    {
        return $this->rows[0] ?? null;
    }

    /**
     * {@inheritdoc}
     */
    public function getAffectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * {@inheritdoc}
     */
    public function getLastInsertId(): int
    {
        return $this->lastInsertId;
    }

    /**
     * {@inheritdoc}
     */
    public function getWarningCount(): int
    {
        return $this->warningCount;
    }

    /**
     * {@inheritdoc}
     */
    public function hasAffectedRows(): bool
    {
        return $this->affectedRows > 0;
    }

    /**
     * {@inheritdoc}
     */
    public function hasLastInsertId(): bool
    {
        return $this->lastInsertId > 0;
    }

    /**
     * {@inheritdoc}
     */
    public function rowCount(): int
    {
        return $this->numRows;
    }

    /**
     * {@inheritdoc}
     */
    public function getColumnCount(): int
    {
        return $this->columnCount;
    }

    /**
     * {@inheritdoc}
     */
    public function isEmpty(): bool
    {
        return $this->numRows === 0;
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator(): \Traversable
    {
        return new \ArrayIterator($this->rows);
    }
}
