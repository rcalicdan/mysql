<?php

namespace Hibla\MysqlClient;

/**
 * Represents the result of a database query that returns rows.
 *
 * This object is iterable and can be used directly in a foreach loop,
 * ensuring backward compatibility.
 */
class Result implements \IteratorAggregate
{
    private int $position = 0;
    private readonly int $rowCount;
    private readonly int $columnCount;

    /**
     * @param  array  $rows  An array of associative arrays representing the result set.
     */
    public function __construct(
        private readonly array $rows
    ) {
        $this->rowCount = count($this->rows);
        $this->columnCount = $this->rowCount > 0 ? count($this->rows[0]) : 0;
    }

    /**
     * Fetches the next row from the result set as an associative array.
     * Returns null if there are no more rows.
     */
    public function fetchAssoc(): ?array
    {
        if ($this->position >= $this->rowCount) {
            return null;
        }

        // Return the row at the current position and then increment the pointer
        return $this->rows[$this->position++];
    }

    /**
     * Fetches all rows from the result set as an array of associative arrays.
     */
    public function fetchAllAssoc(): array
    {
        return $this->rows;
    }

    /**
     * Allows the Result object to be used directly in a foreach loop.
     */
    public function getIterator(): \Traversable
    {
        return new \ArrayIterator($this->rows);
    }

    /**
     * Gets the number of rows in the result set.
     */
    public function getRowCount(): int
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
}
