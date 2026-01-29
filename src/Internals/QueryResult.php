<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

/**
 * Represents the result of a SELECT query that returns rows.
 *
 * This object is optimized for read operations and provides
 * convenient methods for iterating over result sets.
 */
class QueryResult implements \IteratorAggregate, \Countable
{
    private int $position = 0;
    private readonly int $rowCount;
    private readonly int $columnCount;

    /**
     * @param array $rows An array of associative arrays representing the result set.
     */
    public function __construct(
        private readonly array $rows
    ) {
        $this->rowCount = \count($this->rows);
        $this->columnCount = $this->rowCount > 0 ? \count($this->rows[0]) : 0;
    }

    /**
     * Fetches the next row as an associative array.
     * Returns null if there are no more rows.
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
     */
    public function fetchAll(): array
    {
        return $this->rows;
    }

    /**
     * Fetches a single column from all rows.
     *
     * @param string|int $column Column name or index
     * @return array
     */
    public function fetchColumn(string|int $column = 0): array
    {
        return array_map(fn ($row) => $row[$column] ?? null, $this->rows);
    }

    /**
     * Fetches the first row, or null if empty.
     */
    public function fetchOne(): ?array
    {
        return $this->rows[0] ?? null;
    }

    /**
     * Allows iteration in foreach loops.
     */
    public function getIterator(): \Traversable
    {
        return new \ArrayIterator($this->rows);
    }

    /**
     * Gets the number of rows returned.
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
     * Checks if the result set is empty.
     */
    public function isEmpty(): bool
    {
        return $this->rowCount === 0;
    }
}
