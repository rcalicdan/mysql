<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

/**
 * Unified result object for all query types (SELECT, INSERT, UPDATE, DELETE, etc.).
 * 
 * This object contains both row data (for SELECT queries) and execution metadata
 * (for INSERT/UPDATE/DELETE). Methods gracefully handle both cases:
 * - For SELECT: rows are populated, affectedRows is 0
 * - For INSERT/UPDATE/DELETE: rows are empty, affectedRows > 0
 * - For commands like SET: both are typically 0/empty
 */
class Result implements \IteratorAggregate, \Countable
{
    private int $position = 0;
    private readonly int $rowCount;
    private readonly int $columnCount;

    /**
     * @param array $rows Result rows (empty for non-SELECT queries)
     * @param int $affectedRows Number of rows affected (0 for SELECT queries)
     * @param int $lastInsertId Last auto-increment ID (0 if not applicable)
     * @param int $warningCount Number of warnings generated
     * @param array $columns Column names from result set
     */
    public function __construct(
        private readonly array $rows = [],
        private readonly int $affectedRows = 0,
        private readonly int $lastInsertId = 0,
        private readonly int $warningCount = 0,
        private readonly array $columns = []
    ) {
        $this->rowCount = \count($this->rows);
        $this->columnCount = $this->rowCount > 0 ? \count($this->rows[0]) : \count($this->columns);
    }

    /**
     * Fetches the next row as an associative array.
     * Returns null if there are no more rows or for non-SELECT queries.
     */
    public function fetchAssoc(): ?array
    {
        if ($this->position >= $this->rowCount) {
            return null;
        }

        return $this->rows[$this->position++];
    }

    /**
     * Fetches all rows as an array of associative arrays.
     * Returns empty array for non-SELECT queries.
     */
    public function fetchAll(): array
    {
        return $this->rows;
    }

    /**
     * Fetches a single column from all rows.
     * Returns empty array for non-SELECT queries.
     *
     * @param string|int $column Column name or index
     * @return array
     */
    public function fetchColumn(string|int $column = 0): array
    {
        return array_map(fn ($row) => $row[$column] ?? null, $this->rows);
    }

    /**
     * Fetches the first row, or null if empty or non-SELECT.
     */
    public function fetchOne(): ?array
    {
        return $this->rows[0] ?? null;
    }

    /**
     * Gets the number of rows affected by INSERT/UPDATE/DELETE operations.
     * Returns 0 for SELECT queries.
     */
    public function getAffectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * Gets the last inserted auto-increment ID.
     * Returns 0 if not applicable or for SELECT queries.
     */
    public function getLastInsertId(): int
    {
        return $this->lastInsertId;
    }

    /**
     * Gets the number of warnings generated.
     */
    public function getWarningCount(): int
    {
        return $this->warningCount;
    }

    /**
     * Checks if any rows were affected by the operation.
     * For SELECT queries, use count() or isEmpty() instead.
     */
    public function hasAffectedRows(): bool
    {
        return $this->affectedRows > 0;
    }

    /**
     * Checks if an auto-increment ID was generated.
     */
    public function hasLastInsertId(): bool
    {
        return $this->lastInsertId > 0;
    }

    /**
     * Gets the number of rows returned by SELECT queries.
     * For non-SELECT queries, returns 0.
     */
    public function count(): int
    {
        return $this->rowCount;
    }

    /**
     * Gets the number of columns in the result set.
     */
    public function getColumnCount(): int
    {
        return $this->columnCount;
    }

    /**
     * Gets column names from the result set.
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * Checks if the result set is empty (no rows returned).
     */
    public function isEmpty(): bool
    {
        return $this->rowCount === 0;
    }

    /**
     * Allows iteration in foreach loops.
     * For non-SELECT queries, returns empty iterator.
     */
    public function getIterator(): \Traversable
    {
        return new \ArrayIterator($this->rows);
    }
}