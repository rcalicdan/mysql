<?php

use Hibla\Sql\Exceptions\DeadlockException;
use Hibla\Sql\Exceptions\LockWaitTimeoutException;
use Hibla\Sql\TransactionOptions;
use Hibla\Sql\Transaction as TransactionInterface;

use function Hibla\await;

beforeAll(function (): void {
    $client = makeLockClient();

    await($client->execute('DROP TABLE IF EXISTS lock_test'));
    await($client->execute('
            CREATE TABLE lock_test (
                id    INT         NOT NULL PRIMARY KEY,
                value VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB
        '));

    await($client->execute("INSERT INTO lock_test (id, value) VALUES (1, 'row1'), (2, 'row2')"));

    $client->close();
});

afterAll(function (): void {
    $client = makeLockClient();
    await($client->execute('DROP TABLE IF EXISTS lock_test'));
    $client->close();
});

describe('Lock Exceptions', function (): void {
    describe('Exception Detection', function (): void {

        it('throws DeadlockException with code 1213 on a real deadlock', function (): void {
            $db1 = makeLockClient();
            $db2 = makeLockClient();

            $tx1 = await($db1->beginTransaction());
            $tx2 = await($db2->beginTransaction());

            $caughtException = null;

            try {
                await($tx1->execute("SELECT * FROM lock_test WHERE id = 1 FOR UPDATE"));
                await($tx2->execute("SELECT * FROM lock_test WHERE id = 2 FOR UPDATE"));

                $p1 = $tx1->execute("SELECT * FROM lock_test WHERE id = 2 FOR UPDATE");
                $p2 = $tx2->execute("SELECT * FROM lock_test WHERE id = 1 FOR UPDATE");

                await($p1);
                await($p2);
            } catch (\Throwable $e) {
                $caughtException = $e;
            } finally {
                if ($tx1->isActive()) {
                    await($tx1->rollback());
                }
                if ($tx2->isActive()) {
                    await($tx2->rollback());
                }
                $db1->close();
                $db2->close();
            }

            expect($caughtException)->toBeInstanceOf(DeadlockException::class)
                ->and($caughtException->getCode())->toBe(1213)
            ;
        });

        it('throws LockWaitTimeoutException with code 1205 when lock wait expires', function (): void {
            $db1 = makeLockClient();
            $db2 = makeLockClient();

            await($db2->execute('SET SESSION innodb_lock_wait_timeout = 1'));

            $tx1 = await($db1->beginTransaction());
            $tx2 = await($db2->beginTransaction());

            $caughtException = null;

            try {
                await($tx1->execute("SELECT * FROM lock_test WHERE id = 1 FOR UPDATE"));

                await($tx2->execute("SELECT * FROM lock_test WHERE id = 1 FOR UPDATE"));
            } catch (\Throwable $e) {
                $caughtException = $e;
            } finally {
                if ($tx1->isActive()) {
                    await($tx1->rollback());
                }
                if ($tx2->isActive()) {
                    await($tx2->rollback());
                }

                await($db2->execute('SET SESSION innodb_lock_wait_timeout = 50'));

                $db1->close();
                $db2->close();
            }

            expect($caughtException)->toBeInstanceOf(LockWaitTimeoutException::class)
                ->and($caughtException->getCode())->toBe(1205)
            ;
        });
    });

    describe('Retry Behaviour', function (): void {

        it('retries automatically on DeadlockException and succeeds on the next attempt', function (): void {
            $client = makeTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            $result = await($client->transaction(
                function (TransactionInterface $tx) use (&$attemptCount) {
                    $attemptCount++;

                    if ($attemptCount < 2) {
                        throw new DeadlockException('Simulated deadlock', 1213);
                    }

                    return await($tx->fetchOne('SELECT 1 AS val'));
                },
                $options
            ));

            expect($attemptCount)->toBe(2)
                ->and($result['val'])->toBe('1')
            ;

            $client->close();
        });

        it('retries automatically on LockWaitTimeoutException and succeeds on the next attempt', function (): void {
            $client = makeTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            $result = await($client->transaction(
                function (TransactionInterface $tx) use (&$attemptCount) {
                    $attemptCount++;

                    if ($attemptCount < 2) {
                        throw new LockWaitTimeoutException('Simulated lock timeout', 1205);
                    }

                    return await($tx->fetchOne('SELECT 1 AS val'));
                },
                $options
            ));

            expect($attemptCount)->toBe(2)
                ->and($result['val'])->toBe('1')
            ;

            $client->close();
        });

        it('exhausts all attempts on repeated DeadlockException and rethrows the last one', function (): void {
            $client = makeTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            $caughtException = null;

            try {
                await($client->transaction(
                    function () use (&$attemptCount): void {
                        $attemptCount++;
                        throw new DeadlockException('Persistent deadlock', 1213);
                    },
                    $options
                ));
            } catch (\Throwable $e) {
                $caughtException = $e;
            }

            expect($attemptCount)->toBe(3)
                ->and($caughtException)->toBeInstanceOf(DeadlockException::class)
                ->and($caughtException->getCode())->toBe(1213)
            ;

            $client->close();
        });

        it('exhausts all attempts on repeated LockWaitTimeoutException and rethrows the last one', function (): void {
            $client = makeTransactionClient();
            $attemptCount = 0;

            $options = TransactionOptions::default()->withAttempts(3);

            $caughtException = null;

            try {
                await($client->transaction(
                    function () use (&$attemptCount): void {
                        $attemptCount++;
                        throw new LockWaitTimeoutException('Persistent lock timeout', 1205);
                    },
                    $options
                ));
            } catch (\Throwable $e) {
                $caughtException = $e;
            }

            expect($attemptCount)->toBe(3)
                ->and($caughtException)->toBeInstanceOf(LockWaitTimeoutException::class)
                ->and($caughtException->getCode())->toBe(1205)
            ;

            $client->close();
        });
    });
});
