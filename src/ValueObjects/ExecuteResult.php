<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\ValueObjects;

/**
 * Represents the result of a write operation (INSERT, UPDATE, DELETE, etc.).
 * 
 * This object focuses on metadata about the operation rather than row data.
 */
class ExecuteResult
{
    /**
     * @param int $affectedRows Number of rows affected by the operation
     * @param int $lastInsertId The last inserted auto-increment ID (0 if not applicable)
     * @param int $warningCount Number of warnings generated
     */
    public function __construct(
        private readonly int $affectedRows,
        private readonly int $lastInsertId = 0,
        private readonly int $warningCount = 0
    ) {}

    /**
     * Gets the number of rows affected by the operation.
     */
    public function getAffectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * Gets the last inserted auto-increment ID.
     * Returns 0 if not applicable or no auto-increment column.
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
     * Checks if any rows were affected.
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
}