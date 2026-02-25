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
 * This must not be instantiated directly.
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
     * @throws PreparedException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function execute(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<MysqlResult> $promise */
        $promise = $this->statement->execute($params);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<MysqlRowStream>
     * @throws PreparedException If the statement is closed
     * @throws \InvalidArgumentException If parameter count doesn't match
     */
    public function executeStream(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<MysqlRowStream> $promise */
        $promise = $this->statement->executeStream($params);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * Closes the underlying prepared statement and releases the connection
     * back to the pool.
     *
     * NOTE: withCancellation() is intentionally NOT applied here.
     * close() sends COM_STMT_CLOSE on the connection — cancelling it via
     * KILL QUERY would leave the server-side statement handle open and leak
     * server resources. The finally() ensures the connection is always
     * returned to the pool regardless of outcome.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        return $this->statement->close()
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * Bridges cancel() → cancelChain() on a public-facing promise.
     *
     * execute() and executeStream() return the LEAF of an internal promise
     * chain. Calling cancel() on the leaf only cancels that node — it never
     * reaches the ROOT where the real onCancel handler (KILL QUERY dispatch)
     * lives in Connection.
     *
     * This bridge registers an onCancel hook so that cancel() on the leaf
     * immediately walks up to the root via cancelChain(), correctly triggering
     * KILL QUERY on the server.
     *
     * @template T
     * @param PromiseInterface<T> $promise
     * @return PromiseInterface<T>
     */
    private function withCancellation(PromiseInterface $promise): PromiseInterface
    {
        $promise->onCancel($promise->cancelChain(...));

        return $promise;
    }

    /**
     * Releases the connection back to the pool.
     *
     * Guarded by $isReleased to ensure the connection is only released once,
     * even if both close() and __destruct() are triggered.
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
     * Destructor ensures the connection is released when the statement goes
     * out of scope without an explicit close() call.
     */
    public function __destruct()
    {
        if (! $this->isReleased && ! $this->connection->isClosed()) {
            $this->connection->close();
        }

        $this->releaseConnection();
    }
}
