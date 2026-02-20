<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\EventLoop\Loop;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Sql\Exceptions\QueryException;

beforeAll(function (): void {
    $conn = makeConnection();
    await($conn->query('DROP TABLE IF EXISTS cancel_test'));
    await($conn->query('
            CREATE TABLE cancel_test (
                id    INT PRIMARY KEY AUTO_INCREMENT,
                value VARCHAR(255) NOT NULL
            ) ENGINE=InnoDB
        '));
    $conn->close();
});

afterAll(function (): void {
    $conn = makeConnection();
    await($conn->query('DROP TABLE IF EXISTS cancel_test'));
    $conn->close();
});

beforeEach(function (): void {
    $conn = makeConnection();
    await($conn->query('TRUNCATE TABLE cancel_test'));
    $conn->close();
});


describe('Transaction Cancellation', function (): void {
    it('cancels a non-prepared INSERT inside a transaction and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('row1')"));

        $insertPromise = $conn->query('INSERT INTO cancel_test (value) SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($insertPromise): void {
            $insertPromise->cancel();
        });

        expect(fn() => await($insertPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $conn->close();
    });

    test('rollback after a cancelled INSERT inside a transaction persists no rows', function (): void {
        $conn = makeConnection();

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('row1')"));

        $insertPromise = $conn->query('INSERT INTO cancel_test (value) SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($insertPromise): void {
            $insertPromise->cancel();
        });

        expect(fn() => await($insertPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            try {
                await($conn->query('DO SLEEP(0)'));
            } catch (QueryException) {
                // Expected: stale ERR packet may arrive asynchronously during absorption
            }
            $conn->clearCancelledFlag();
        }

        await($conn->query('ROLLBACK'));

        $result = await($conn->query('SELECT COUNT(*) AS count FROM cancel_test'));

        expect((int) $result->fetchOne()['count'])->toBe(0);

        $conn->close();
    });


    it('cancels a prepared INSERT inside a transaction and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('prepared_row1')"));

        $stmt = await($conn->prepare('INSERT INTO cancel_test (value) SELECT SLEEP(?)'));
        $execPromise = $stmt->execute([10]);

        Loop::addTimer(2.0, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn() => await($execPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        await($conn->query('ROLLBACK'));
        await($stmt->close());
        $conn->close();
    });

    test('transaction can continue and commit after a cancelled prepared INSERT', function (): void {
        $conn = makeConnection();

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('prepared_row1')"));

        $stmt = await($conn->prepare('INSERT INTO cancel_test (value) SELECT SLEEP(?)'));
        $execPromise = $stmt->execute([10]);

        Loop::addTimer(2.0, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn() => await($execPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            try {
                await($conn->query('DO SLEEP(0)'));
            } catch (QueryException) {
                // Expected: stale ERR packet may arrive asynchronously during absorption
            }
            $conn->clearCancelledFlag();
        }

        await($conn->query("INSERT INTO cancel_test (value) VALUES ('after_cancel')"));
        await($conn->query('COMMIT'));

        $result = await($conn->query('SELECT COUNT(*) AS count FROM cancel_test'));
        $count = (int) $result->fetchOne()['count'];

        // 2 rows minimum (prepared_row1 + after_cancel)
        // 3 rows possible if SLEEP returned 1 before KILL arrived
        expect($count)->toBeGreaterThanOrEqual(2)
            ->and($count)->toBeLessThanOrEqual(3)
        ;

        await($stmt->close());
        $conn->close();
    });

    it('cancels a streaming SELECT inside a transaction and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('stream_test1'), ('stream_test2'), ('stream_test3')"));

        $streamPromise = $conn->streamQuery('SELECT value, SLEEP(10) AS delay FROM cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        await($conn->query('ROLLBACK'));
        $conn->close();
    });

    test('connection can still query after a cancelled stream inside a transaction', function (): void {
        $conn = makeConnection();

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('stream_test1'), ('stream_test2'), ('stream_test3')"));

        $streamPromise = $conn->streamQuery('SELECT value, SLEEP(10) AS delay FROM cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            try {
                await($conn->query('DO SLEEP(0)'));
            } catch (QueryException) {
                // Expected: stale ERR packet may arrive asynchronously during absorption
            }
            $conn->clearCancelledFlag();
        }

        // Should still be able to query within the same transaction
        $result = await($conn->query('SELECT COUNT(*) AS count FROM cancel_test'));

        expect((int) $result->fetchOne()['count'])->toBe(3);

        await($conn->query('ROLLBACK'));
        $conn->close();
    });

    test('rollback after a cancelled stream inside a transaction persists no rows', function (): void {
        $conn = makeConnection();

        await($conn->query('START TRANSACTION'));
        await($conn->query("INSERT INTO cancel_test (value) VALUES ('stream_test1'), ('stream_test2'), ('stream_test3')"));

        $streamPromise = $conn->streamQuery('SELECT value, SLEEP(10) AS delay FROM cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            try {
                await($conn->query('DO SLEEP(0)'));
            } catch (QueryException) {
                // Expected: stale ERR packet may arrive asynchronously during absorption
            }
            $conn->clearCancelledFlag();
        }

        await($conn->query('ROLLBACK'));

        $result = await($conn->query('SELECT COUNT(*) AS count FROM cancel_test'));

        expect((int) $result->fetchOne()['count'])->toBe(0);

        $conn->close();
    });

    it('cancels an UPDATE inside a transaction and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        await($conn->query("INSERT INTO cancel_test (value) VALUES ('original1'), ('original2')"));

        await($conn->query('START TRANSACTION'));

        $updatePromise = $conn->query("UPDATE cancel_test SET value = 'updated' WHERE id > 0 AND SLEEP(10) = 0");

        Loop::addTimer(2.0, function () use ($updatePromise): void {
            $updatePromise->cancel();
        });

        expect(fn() => await($updatePromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        await($conn->query('ROLLBACK'));
        $conn->close();
    });

    test('can perform another UPDATE after a cancelled UPDATE inside a transaction', function (): void {
        $conn = makeConnection();

        await($conn->query("INSERT INTO cancel_test (value) VALUES ('original1'), ('original2')"));

        await($conn->query('START TRANSACTION'));

        $updatePromise = $conn->query("UPDATE cancel_test SET value = 'updated' WHERE id > 0 AND SLEEP(10) = 0");

        Loop::addTimer(2.0, function () use ($updatePromise): void {
            $updatePromise->cancel();
        });

        expect(fn() => await($updatePromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            try {
                await($conn->query('DO SLEEP(0)'));
            } catch (QueryException) {
                // Expected: stale ERR packet may arrive asynchronously during absorption
            }
            $conn->clearCancelledFlag();
        }

        $result = await($conn->query("UPDATE cancel_test SET value = 'final_value' WHERE id = 1"));

        expect($result->getAffectedRows())->toBe(1);

        await($conn->query('ROLLBACK'));
        $conn->close();
    });

    test('rollback after a cancelled UPDATE restores original row values', function (): void {
        $conn = makeConnection();

        await($conn->query("INSERT INTO cancel_test (value) VALUES ('original1'), ('original2')"));

        await($conn->query('START TRANSACTION'));

        $updatePromise = $conn->query("UPDATE cancel_test SET value = 'updated' WHERE id > 0 AND SLEEP(10) = 0");

        Loop::addTimer(2.0, function () use ($updatePromise): void {
            $updatePromise->cancel();
        });

        expect(fn() => await($updatePromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            try {
                await($conn->query('DO SLEEP(0)'));
            } catch (QueryException) {
                // Expected: stale ERR packet may arrive asynchronously during absorption
            }
            $conn->clearCancelledFlag();
        }

        await($conn->query('ROLLBACK'));

        $result = await($conn->query('SELECT value FROM cancel_test ORDER BY id'));
        $values = array_column($result->fetchAll(), 'value');

        expect($values)->toBe(['original1', 'original2']);

        $conn->close();
    });
});
