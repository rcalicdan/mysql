<?php

declare(strict_types=1);

namespace Hibla\Mysql\Internals;

use Hibla\Mysql\Interfaces\MysqlResult;
use Hibla\Mysql\Interfaces\MysqlRowStream;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;

/**
 * A wrapper around PreparedStatement used strictly inside Transactions.
 *
 * This class automatically sends COM_STMT_CLOSE to the server when the
 * statement is closed or goes out of scope (garbage collected), preventing
 * server-side memory leaks.
 *
 * Crucially, unlike ManagedPreparedStatement, this DOES NOT release the
 * underlying TCP connection back to the pool, as the Transaction still owns it.
 *
 * @internal
 */
class TransactionPreparedStatement implements PreparedStatementInterface
{
    private bool $isClosed = false;

    public function __construct(
        private readonly PreparedStatementInterface $statement,
        private readonly Connection $connection
    ) {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<MysqlResult>
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
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            // @phpstan-ignore-next-line
            return Promise::resolved(null);
        }

        $this->isClosed = true;

        return $this->statement->close();
    }

    /**
     * Bridges cancel() → cancelChain() on a public-facing promise.
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
     * Destructor ensures the server-side statement is closed when the object
     * goes out of scope.
     */
    public function __destruct()
    {
        if (! $this->isClosed && ! $this->connection->isClosed()) {
            $this->close();
        }
    }
}
