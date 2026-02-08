<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Sql\Exceptions\PreparedException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;

/**
 * A wrapper around PreparedStatement that manages connection lifecycle.
 *
 * This class automatically releases the connection back to the pool when
 * the statement is closed or goes out of scope.
 *
 * this must not be instantiated directly
 */
class ManagedPreparedStatement implements PreparedStatementInterface
{
    private bool $isReleased = false;

    /**
     * @param PreparedStatementInterface $statement The underlying prepared statement
     * @param Connection $connection The connection this statement belongs to
     * @param PoolManager $pool The pool to release the connection back to
     */
    public function __construct(
        private readonly PreparedStatementInterface $statement,
        private readonly Connection $connection,
        private readonly PoolManager $pool
    ) {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<MysqlResult>
     */
    public function execute(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<MysqlResult>*/
        return $this->statement->execute($params);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<MysqlRowStream>
     * @throws PreparedException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function executeStream(
        array $params = [],
    ): PromiseInterface {
        /** @var PromiseInterface<MysqlRowStream>  */
        return $this->statement->executeStream($params);
    }

    /**
     * {@inheritdoc}
     */
    public function close(): PromiseInterface
    {
        return $this->statement->close()
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * Release the connection back to the pool.
     *
     * @return void
     */
    private function releaseConnection(): void
    {
        if ($this->isReleased) {
            return;
        }

        $this->isReleased = true;
        $this->pool->release($this->connection);
    }

    /**
     * Destructor ensures the connection is released when the statement goes out of scope.
     */
    public function __destruct()
    {
        $this->releaseConnection();
    }
}
