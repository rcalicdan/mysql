<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Interfaces\TransactionInterface;
use Hibla\Promise\Interfaces\PromiseInterface;

class Transaction implements TransactionInterface
{
    private bool $isActive = true;

    /**
     * @param MysqlConnection $connection The parent connection.
     * @param \Closure $releaseCallback The function to call to unlock the connection.
     */
    public function __construct(
        private readonly MysqlConnection $connection,
        private readonly \Closure $releaseCallback
    ) {}

    public function query(string $sql): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active (already committed or rolled back).");
        }
        return $this->connection->query($sql);
    }

    public function execute(string $sql): PromiseInterface
    {
        return $this->query($sql);
    }

    public function prepare(string $sql): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active.");
        }
        return $this->connection->prepare($sql);
    }

    public function commit(): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active.");
        }
        $this->isActive = false;

        return $this->query('COMMIT')->then(function ($result) {
            ($this->releaseCallback)(); // Unlock the connection
            return $result;
        });
    }

    public function rollback(): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active.");
        }
        $this->isActive = false;

        return $this->query('ROLLBACK')->then(function ($result) {
            ($this->releaseCallback)(); // Unlock the connection
            return $result;
        });
    }

    public function savepoint(string $identifier): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active.");
        }
        return $this->query("SAVEPOINT `$identifier`");
    }

    public function rollbackTo(string $identifier): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active.");
        }
        return $this->query("ROLLBACK TO `$identifier`");
    }

    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        if (!$this->isActive) {
            throw new \RuntimeException("Transaction is no longer active.");
        }
        return $this->query("RELEASE SAVEPOINT `$identifier`");
    }

    public function isActive(): bool
    {
        return $this->isActive && !$this->connection->isClosed();
    }

    public function isClosed(): bool
    {
        return $this->connection->isClosed();
    }
}