<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Interfaces\MysqlResult;

/**
 * Unified result object for all query types (SELECT, INSERT, UPDATE, DELETE, etc.).
 *
 * This object contains both row data (for SELECT queries) and execution metadata
 * (for INSERT/UPDATE/DELETE). Methods gracefully handle both cases:
 * - For SELECT: rows are populated, affectedRows is 0
 * - For INSERT/UPDATE/DELETE: rows are empty, affectedRows > 0
 * - For commands like SET: both are typically 0/empty
 *
 * @internal This must not be use diretly.
 */
class Result implements MysqlResult
{
    private int $position = 0;
    private readonly int $numRows;
    private readonly int $columnCount;

    /**
     * @param array<int, array<string, mixed>> $rows Result rows (empty for non-SELECT queries)
     * @param int $affectedRows Number of rows affected (0 for SELECT queries)
     * @param int $lastInsertId Last auto-increment ID (0 if not applicable)
     * @param int $warningCount Number of warnings generated
     * @param array<int, string> $columns Column names from result set
     */
    public function __construct(
        private readonly array $rows = [],
        private readonly int $affectedRows = 0,
        private readonly int $lastInsertId = 0,
        private readonly int $warningCount = 0,
        private readonly array $columns = []
    ) {
        $this->numRows = \count($this->rows);
        $this->columnCount = $this->numRows > 0 ? \count($this->rows[0]) : \count($this->columns);
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
    public function getColumns(): array
    {
        return $this->columns;
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
