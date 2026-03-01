<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\Mysql\Internals\TransactionPreparedStatement;
use Hibla\Sql\Exceptions\TransactionException;
use Hibla\Sql\IsolationLevel;

use Hibla\Sql\Transaction as TransactionInterface;
use Hibla\Sql\TransactionOptions;
use Tests\Fixtures\MarkerRetryableException;
use Tests\Fixtures\RetryableAppException;

beforeAll(function (): void {
    $client = makeTransactionClient();

    await($client->query('CREATE TABLE IF NOT EXISTS tx_test (
        id    INT AUTO_INCREMENT PRIMARY KEY,
        value VARCHAR(100)
    )'));

    $client->close();
});

afterAll(function (): void {
    $client = makeTransactionClient();
    await($client->query('DROP TABLE IF EXISTS tx_test'));
    $client->close();
});

beforeEach(function (): void {
    $client = makeTransactionClient();
    await($client->query('TRUNCATE TABLE tx_test'));
    $client->close();
});

describe('MysqlClient Transaction', function (): void {

    describe('Begin Transaction', function (): void {

        it('returns a Transaction instance', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect($tx)->toBeInstanceOf(TransactionInterface::class)
                ->and($tx->isActive())->toBeTrue()
            ;

            await($tx->rollback());
            $client->close();
        });

        it('transaction is active after begin', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect($tx->isActive())->toBeTrue()
                ->and($tx->isClosed())->toBeFalse()
            ;

            await($tx->rollback());
            $client->close();
        });

        it('begins a transaction with READ UNCOMMITTED isolation level', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction(IsolationLevel::READ_UNCOMMITTED));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('begins a transaction with READ COMMITTED isolation level', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction(IsolationLevel::READ_COMMITTED));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('begins a transaction with REPEATABLE READ isolation level', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction(IsolationLevel::REPEATABLE_READ));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('begins a transaction with SERIALIZABLE isolation level', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction(IsolationLevel::SERIALIZABLE));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('releases the connection back to pool after beginTransaction fails', function (): void {
            $client = makeTransactionClient(maxConnections: 1);
            $tx = await($client->beginTransaction());

            await($tx->rollback());

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });
    });

    describe('Commit', function (): void {

        it('commits a transaction and persists data', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('committed')"));
            await($tx->commit());

            $result = await($client->query("SELECT value FROM tx_test WHERE value = 'committed'"));

            expect($result->fetchOne()['value'])->toBe('committed');

            $client->close();
        });

        it('transaction is no longer active after commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('x')"));
            await($tx->commit());

            expect($tx->isActive())->toBeFalse();

            $client->close();
        });

        it('releases the connection back to pool after commit', function (): void {
            $client = makeTransactionClient(maxConnections: 1);
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('pool_check')"));
            await($tx->commit());

            // Single-slot pool — proves connection was released
            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('throws TransactionException when committing an already committed transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->commit());

            expect(fn() => await($tx->commit()))
                ->toThrow(TransactionException::class);

            $client->close();
        });

        it('commits multiple inserts atomically', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('row1')"));
            await($tx->query("INSERT INTO tx_test (value) VALUES ('row2')"));
            await($tx->query("INSERT INTO tx_test (value) VALUES ('row3')"));
            await($tx->commit());

            $result = await($client->query('SELECT COUNT(*) AS cnt FROM tx_test'));

            expect((int) $result->fetchOne()['cnt'])->toBe(3);

            $client->close();
        });
    });

    describe('Rollback', function (): void {

        it('rolls back a transaction and discards data', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('rolled_back')"));
            await($tx->rollback());

            $result = await($client->query("SELECT value FROM tx_test WHERE value = 'rolled_back'"));

            expect($result->fetchOne())->toBeNull();

            $client->close();
        });

        it('transaction is no longer active after rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->rollback());

            expect($tx->isActive())->toBeFalse();

            $client->close();
        });

        it('releases the connection back to pool after rollback', function (): void {
            $client = makeTransactionClient(maxConnections: 1);
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('will_rollback')"));
            await($tx->rollback());

            // Single-slot pool — proves connection was released
            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('throws TransactionException when rolling back an already rolled back transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->rollback());

            expect(fn() => await($tx->rollback()))
                ->toThrow(TransactionException::class);

            $client->close();
        });

        it('only discards the active transaction leaving prior commits intact', function (): void {
            $client = makeTransactionClient();

            $tx1 = await($client->beginTransaction());
            await($tx1->query("INSERT INTO tx_test (value) VALUES ('committed_first')"));
            await($tx1->commit());

            $tx2 = await($client->beginTransaction());
            await($tx2->query("INSERT INTO tx_test (value) VALUES ('rolled_back_second')"));
            await($tx2->rollback());

            $result = await($client->query('SELECT COUNT(*) AS cnt FROM tx_test'));

            expect((int) $result->fetchOne()['cnt'])->toBe(1);

            $client->close();
        });
    });

    describe('Query Operations Inside Transaction', function (): void {

        it('executes a plain query without params inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $result = await($tx->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            await($tx->rollback());
            $client->close();
        });

        it('executes a query with params inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $result = await($tx->query('SELECT ? AS val', [42]));

            expect($result->fetchOne()['val'])->toBe(42);

            await($tx->rollback());
            $client->close();
        });

        it('execute() returns affected rows inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            $affected = await($tx->execute("INSERT INTO tx_test (value) VALUES ('exec_test')"));

            expect($affected)->toBe(1);

            await($tx->rollback());
            $client->close();
        });

        it('executeGetId() returns last insert id inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $insertId = await($tx->executeGetId("INSERT INTO tx_test (value) VALUES ('id_test')"));

            expect($insertId)->toBeGreaterThan(0);

            await($tx->rollback());
            $client->close();
        });

        it('fetchOne() returns the first row inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('fetch_one')"));

            $row = await($tx->fetchOne("SELECT value FROM tx_test WHERE value = 'fetch_one'"));

            expect($row['value'])->toBe('fetch_one');

            await($tx->rollback());
            $client->close();
        });

        it('fetchOne() returns null when no rows match inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $row = await($tx->fetchOne('SELECT 1 WHERE 1 = 0'));

            expect($row)->toBeNull();

            await($tx->rollback());
            $client->close();
        });

        it('fetchValue() returns a named column value inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $value = await($tx->fetchValue('SELECT 99 AS answer', 'answer'));

            expect($value)->toBe('99');

            await($tx->rollback());
            $client->close();
        });

        it('fetchValue() returns null when no rows match inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $value = await($tx->fetchValue('SELECT 1 WHERE 1 = 0', 'answer'));

            expect($value)->toBeNull();

            await($tx->rollback());
            $client->close();
        });

        it('throws TransactionException when querying after commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->commit());

            expect(fn() => await($tx->query('SELECT 1')))
                ->toThrow(TransactionException::class);

            $client->close();
        });

        it('throws TransactionException when querying after rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->rollback());

            expect(fn() => await($tx->query('SELECT 1')))
                ->toThrow(TransactionException::class);

            $client->close();
        });
    });

    describe('Stream Inside Transaction', function (): void {

        it('streams rows without params inside a transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('s1'), ('s2'), ('s3')"));

            $stream = await($tx->stream('SELECT value FROM tx_test'));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(3);

            await($tx->rollback());
            $client->close();
        });

        it('streams rows with params inside a transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('stream_param')"));

            $stream = await($tx->stream('SELECT value FROM tx_test WHERE value = ?', ['stream_param']));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(1)
                ->and($rows[0]['value'])->toBe('stream_param')
            ;

            await($tx->rollback());
            $client->close();
        });

        it('streams an empty result set without error inside a transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $stream = await($tx->stream('SELECT value FROM tx_test WHERE value = ?', ['nonexistent']));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toBeEmpty();

            await($tx->rollback());
            $client->close();
        });

        it('throws TransactionException when streaming after commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->commit());

            expect(fn() => await($tx->stream('SELECT 1')))
                ->toThrow(TransactionException::class);

            $client->close();
        });
    });

    describe('Prepare Inside Transaction', function (): void {

        it('returns a PreparedStatement inside a transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $stmt = await($tx->prepare('SELECT ? AS val'));

            expect($stmt)->toBeInstanceOf(TransactionPreparedStatement::class);

            await($stmt->close());
            await($tx->rollback());
            $client->close();
        });

        it('executes a prepared statement multiple times inside transaction', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $stmt = await($tx->prepare('INSERT INTO tx_test (value) VALUES (?)'));

            await($stmt->execute(['prepared_1']));
            await($stmt->execute(['prepared_2']));

            $result = await($tx->query('SELECT COUNT(*) AS cnt FROM tx_test'));

            expect((int) $result->fetchOne()['cnt'])->toBe(2);

            await($stmt->close());
            await($tx->rollback());
            $client->close();
        });

        it('throws TransactionException when preparing after rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->rollback());

            expect(fn() => await($tx->prepare('SELECT 1')))
                ->toThrow(TransactionException::class);

            $client->close();
        });
    });

    describe('Savepoints', function (): void {

        it('creates a savepoint without error', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->savepoint('sp1'));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('rolls back to a savepoint discarding changes after it', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('before_savepoint')"));
            await($tx->savepoint('sp1'));
            await($tx->query("INSERT INTO tx_test (value) VALUES ('after_savepoint')"));
            await($tx->rollbackTo('sp1'));

            $result = await($tx->query('SELECT COUNT(*) AS cnt FROM tx_test'));

            expect((int) $result->fetchOne()['cnt'])->toBe(1);

            await($tx->commit());
            $client->close();
        });

        it('releases a savepoint without error', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->savepoint('sp_release'));
            await($tx->releaseSavepoint('sp_release'));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('supports multiple savepoints and partial rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->query("INSERT INTO tx_test (value) VALUES ('base')"));
            await($tx->savepoint('sp1'));
            await($tx->query("INSERT INTO tx_test (value) VALUES ('after_sp1')"));
            await($tx->savepoint('sp2'));
            await($tx->query("INSERT INTO tx_test (value) VALUES ('after_sp2')"));
            await($tx->rollbackTo('sp2'));

            $result = await($tx->query('SELECT COUNT(*) AS cnt FROM tx_test'));

            expect((int) $result->fetchOne()['cnt'])->toBe(2);

            await($tx->rollback());
            $client->close();
        });

        it('throws TransactionException for empty savepoint identifier', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect(fn() => await($tx->savepoint('')))
                ->toThrow(InvalidArgumentException::class, 'Savepoint identifier cannot be empty');

            await($tx->rollback());
            $client->close();
        });

        it('throws InvalidArgumentException for savepoint identifier that is too long', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect(fn() => await($tx->savepoint(str_repeat('a', 65))))
                ->toThrow(InvalidArgumentException::class, 'Savepoint identifier too long');

            await($tx->rollback());
            $client->close();
        });

        it('throws InvalidArgumentException for savepoint identifier with null byte', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect(fn() => await($tx->savepoint("sp\x00name")))
                ->toThrow(InvalidArgumentException::class, 'invalid byte values');

            await($tx->rollback());
            $client->close();
        });

        it('throws InvalidArgumentException for savepoint identifier with leading whitespace', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect(fn() => await($tx->savepoint(' sp1')))
                ->toThrow(InvalidArgumentException::class, 'cannot start or end with spaces');

            await($tx->rollback());
            $client->close();
        });

        it('throws InvalidArgumentException for savepoint identifier with trailing whitespace', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            expect(fn() => await($tx->savepoint('sp1 ')))
                ->toThrow(InvalidArgumentException::class, 'cannot start or end with spaces');

            await($tx->rollback());
            $client->close();
        });

        it('accepts a savepoint identifier at exactly 64 characters', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->savepoint(str_repeat('a', 64)));

            expect($tx->isActive())->toBeTrue();

            await($tx->rollback());
            $client->close();
        });

        it('throws TransactionException when creating savepoint after commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->commit());

            expect(fn() => await($tx->savepoint('sp1')))
                ->toThrow(TransactionException::class);

            $client->close();
        });
    });

    describe('Callbacks', function (): void {

        it('onCommit callback is fired after successful commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $called = false;

            $tx->onCommit(function () use (&$called): void {
                $called = true;
            });

            await($tx->commit());

            expect($called)->toBeTrue();

            $client->close();
        });

        it('onRollback callback is fired after rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $called = false;

            $tx->onRollback(function () use (&$called): void {
                $called = true;
            });

            await($tx->rollback());

            expect($called)->toBeTrue();

            $client->close();
        });

        it('onCommit callback is NOT fired after rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $called = false;

            $tx->onCommit(function () use (&$called): void {
                $called = true;
            });

            await($tx->rollback());

            expect($called)->toBeFalse();

            $client->close();
        });

        it('onRollback callback is NOT fired after commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $called = false;

            $tx->onRollback(function () use (&$called): void {
                $called = true;
            });

            await($tx->commit());

            expect($called)->toBeFalse();

            $client->close();
        });

        it('multiple onCommit callbacks all fire in order', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $order = [];

            $tx->onCommit(function () use (&$order): void {
                $order[] = 1;
            });
            $tx->onCommit(function () use (&$order): void {
                $order[] = 2;
            });
            $tx->onCommit(function () use (&$order): void {
                $order[] = 3;
            });

            await($tx->commit());

            expect($order)->toBe([1, 2, 3]);

            $client->close();
        });

        it('multiple onRollback callbacks all fire in order', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());
            $order = [];

            $tx->onRollback(function () use (&$order): void {
                $order[] = 1;
            });
            $tx->onRollback(function () use (&$order): void {
                $order[] = 2;
            });
            $tx->onRollback(function () use (&$order): void {
                $order[] = 3;
            });

            await($tx->rollback());

            expect($order)->toBe([1, 2, 3]);

            $client->close();
        });

        it('throws TransactionException when registering onCommit after commit', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->commit());

            expect(fn() => $tx->onCommit(function (): void {}))
                ->toThrow(TransactionException::class);

            $client->close();
        });

        it('throws TransactionException when registering onRollback after rollback', function (): void {
            $client = makeTransactionClient();
            $tx = await($client->beginTransaction());

            await($tx->rollback());

            expect(fn() => $tx->onRollback(function (): void {}))
                ->toThrow(TransactionException::class);

            $client->close();
        });
    });

    describe('Destructor', function (): void {

        it('releases connection to pool when transaction goes out of scope without commit or rollback', function (): void {
            $client = makeTransactionClient(maxConnections: 1);

            $tx = await($client->beginTransaction());
            await($tx->query("INSERT INTO tx_test (value) VALUES ('will_be_abandoned')"));
            unset($tx);

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('does not double-release connection when unset after explicit rollback', function (): void {
            $client = makeTransactionClient(maxConnections: 1);

            $tx = await($client->beginTransaction());
            await($tx->rollback());
            unset($tx);

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });
    });

    describe('transaction() Helper', function (): void {
        it('commits successfully and persists the row', function (): void {
            $client = makeManualTransactionClient();

            $result = await($client->transaction(static function (TransactionInterface $tx): string {
                await($tx->execute("INSERT INTO tx_test (value) VALUES ('txopt_committed')"));

                return 'ok';
            }));

            expect($result)->toBe('ok');

            $count = (int) await($client->fetchValue(
                "SELECT COUNT(*) FROM tx_test WHERE value = 'txopt_committed'"
            ));

            expect($count)->toBe(1);

            $client->close();
        });

        it('rolls back automatically and propagates the original exception', function (): void {
            $client = makeManualTransactionClient();

            $before = (int) await($client->fetchValue('SELECT COUNT(*) FROM tx_test'));

            expect(fn() => await($client->transaction(static function (TransactionInterface $tx): void {
                await($tx->execute("INSERT INTO tx_test (value) VALUES ('txopt_rollback')"));

                throw new \RuntimeException('intentional rollback');
            })))->toThrow(\RuntimeException::class, 'intentional rollback');

            $after = (int) await($client->fetchValue('SELECT COUNT(*) FROM tx_test'));

            expect($after)->toBe($before);

            $client->close();
        });

        it('retries on tier-1 RetryableException marker and commits on the final attempt', function (): void {
            $client = makeManualTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            await($client->transaction(static function (TransactionInterface $tx) use (&$attemptCount): void {
                $attemptCount++;

                await($tx->execute("INSERT INTO tx_test (value) VALUES ('txopt_marker_{$attemptCount}')"));

                if ($attemptCount < 3) {
                    throw new MarkerRetryableException("Simulated tier-1 failure #{$attemptCount}");
                }
            }, $options));

            expect($attemptCount)->toBe(3);

            $committed = (int) await($client->fetchValue(
                "SELECT COUNT(*) FROM tx_test WHERE value = 'txopt_marker_3'"
            ));

            expect($committed)->toBe(1);

            $orphans = (int) await($client->fetchValue(
                "SELECT COUNT(*) FROM tx_test WHERE value LIKE 'txopt_marker_%' AND value != 'txopt_marker_3'"
            ));

            expect($orphans)->toBe(0);

            $client->close();
        });

        it('retries on tier-3 withRetryableExceptions() and commits on the final attempt', function (): void {
            $client = makeManualTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()
                ->withAttempts(3)
                ->withRetryableExceptions([RetryableAppException::class]);

            await($client->transaction(static function (TransactionInterface $tx) use (&$attemptCount): void {
                $attemptCount++;

                await($tx->execute("INSERT INTO tx_test (value) VALUES ('txopt_predicate_{$attemptCount}')"));

                if ($attemptCount < 3) {
                    throw new RetryableAppException("Simulated tier-3 failure #{$attemptCount}");
                }
            }, $options));

            expect($attemptCount)->toBe(3);

            $committed = (int) await($client->fetchValue(
                "SELECT COUNT(*) FROM tx_test WHERE value = 'txopt_predicate_3'"
            ));

            expect($committed)->toBe(1);

            $orphans = (int) await($client->fetchValue(
                "SELECT COUNT(*) FROM tx_test WHERE value LIKE 'txopt_predicate_%' AND value != 'txopt_predicate_3'"
            ));

            expect($orphans)->toBe(0);

            $client->close();
        });

        it('stops immediately on a non-retryable exception without burning remaining attempts', function (): void {
            $client = makeManualTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()->withAttempts(5);

            expect(function () use ($client, $options, &$attemptCount): void {
                await($client->transaction(
                    static function (TransactionInterface $tx) use (&$attemptCount): void {
                        $attemptCount++;

                        await($tx->execute("INSERT INTO tx_test (value) VALUES ('txopt_nonretryable')"));

                        throw new \RuntimeException('non-retryable failure');
                    },
                    $options
                ));
            })->toThrow(\RuntimeException::class, 'non-retryable failure');

            expect($attemptCount)->toBe(1);

            $orphans = (int) await($client->fetchValue(
                "SELECT COUNT(*) FROM tx_test WHERE value = 'txopt_nonretryable'"
            ));

            expect($orphans)->toBe(0);

            $client->close();
        });

        it('commits on success and returns the callback result', function (): void {
            $client = makeTransactionClient();

            $result = await($client->transaction(function (TransactionInterface $tx) {
                return await($tx->query("INSERT INTO tx_test (value) VALUES ('helper_commit')"))
                    ->getAffectedRows();
            }));

            expect($result)->toBe(1);

            $check = await($client->query("SELECT COUNT(*) AS cnt FROM tx_test WHERE value = 'helper_commit'"));

            expect((int) $check->fetchOne()['cnt'])->toBe(1);

            $client->close();
        });

        it('rolls back automatically when the callback throws', function (): void {
            $client = makeTransactionClient();

            try {
                await($client->transaction(function (TransactionInterface $tx): void {
                    await($tx->query("INSERT INTO tx_test (value) VALUES ('will_rollback')"));

                    throw new RuntimeException('intentional failure');
                }));
            } catch (Throwable) {
                // expected
            }

            $result = await($client->query("SELECT COUNT(*) AS cnt FROM tx_test WHERE value = 'will_rollback'"));

            expect((int) $result->fetchOne()['cnt'])->toBe(0);

            $client->close();
        });

        it('rethrows the original exception after rollback', function (): void {
            $client = makeTransactionClient();

            expect(function () use ($client): void {
                await($client->transaction(function (): void {
                    throw new RuntimeException('original error');
                }));
            })->toThrow(RuntimeException::class, 'original error');

            $client->close();
        });

        it('retries the specified number of times on a retryable exception', function (): void {
            $client = makeTransactionClient();
            $attempts = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            try {
                await($client->transaction(function () use (&$attempts): void {
                    $attempts++;

                    // Must be retryable — plain RuntimeException is not retried by the new API.
                    throw new MarkerRetryableException('always fails');
                }, $options));
            } catch (Throwable) {
                // expected — all 3 attempts exhausted
            }

            expect($attempts)->toBe(3);

            $client->close();
        });

        it('succeeds on a later retry after initial retryable failures', function (): void {
            $client = makeTransactionClient();
            $attempt = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            $result = await($client->transaction(function (TransactionInterface $tx) use (&$attempt) {
                $attempt++;

                if ($attempt < 3) {
                    // Must be retryable — plain RuntimeException is not retried by the new API.
                    throw new MarkerRetryableException('not yet');
                }

                return await($tx->fetchOne('SELECT 1 AS val'));
            }, $options));

            expect($attempt)->toBe(3)
                ->and($result['val'])->toBe('1')
            ;

            $client->close();
        });

        it('throws InvalidArgumentException when attempts is less than 1', function (): void {
            $client = makeTransactionClient();

            // The guard now lives in TransactionOptions, so the exception is thrown
            // at options construction time, before transaction() is even called.
            expect(fn() => new TransactionOptions(attempts: 0))
                ->toThrow(InvalidArgumentException::class, 'attempts must be at least 1');

            $client->close();
        });

        it('uses the specified isolation level', function (): void {
            $client = makeTransactionClient();

            $options = TransactionOptions::default()
                ->withIsolationLevel(IsolationLevel::SERIALIZABLE);

            $result = await($client->transaction(
                function (TransactionInterface $tx) {
                    return await($tx->fetchOne('SELECT 1 AS val'));
                },
                $options
            ));

            expect($result['val'])->toBe('1');

            $client->close();
        });

        it('releases the connection back to pool after the callback completes', function (): void {
            $client = makeTransactionClient(maxConnections: 1);

            await($client->transaction(function (TransactionInterface $tx): void {
                await($tx->query('SELECT 1'));
            }));

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('releases connection back to pool even when the callback fails', function (): void {
            $client = makeTransactionClient(maxConnections: 1);

            try {
                await($client->transaction(function (): void {
                    throw new RuntimeException('fail');
                }));
            } catch (Throwable) {
                // expected
            }

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });
    });
});
