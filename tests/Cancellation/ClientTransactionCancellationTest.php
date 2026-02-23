<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\EventLoop\Loop;
use Hibla\Promise\Exceptions\CancelledException;

beforeEach(function (): void {
    $client = makeTransactionClient();
    await($client->query('DROP TABLE IF EXISTS client_cancel_test'));
    await($client->query('
        CREATE TABLE client_cancel_test (
            id    INT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB
    '));
    $client->close();
});

afterEach(function (): void {
    $client = makeTransactionClient();
    await($client->query('DROP TABLE IF EXISTS client_cancel_test'));
    $client->close();
});

describe('Transaction Non-Prepared Query Cancellation', function (): void {

    it('cancels a non-prepared query inside a transaction and throws CancelledException or QueryException', function (): void {
        $client = makeTransactionClient();
        $startTime = microtime(true);

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('row1')"));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        expect(fn () => await($slowPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $client->close();
    });

    it('allows rollback after cancelling a non-prepared query inside a transaction', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('row1')"));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        try {
            await($slowPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->rollback());
        }

        $result = await($client->query('SELECT COUNT(*) as count FROM client_cancel_test'));
        expect((int) $result->fetchOne()['count'])->toBe(0);

        $client->close();
    });

    test('pool connection is healthy after cancellation and rollback of a non-prepared transaction query', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('row1')"));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        try {
            await($slowPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->rollback());
        }

        $result = await($client->query('SELECT "Alive" AS status'));
        expect($result->fetchOne()['status'])->toBe('Alive');

        $client->close();
    });
});

describe('Transaction Prepared Query Cancellation', function (): void {

    it('cancels a prepared query inside a transaction and throws CancelledException or QueryException', function (): void {
        $client = makeTransactionClient();
        $startTime = microtime(true);

        $tx = await($client->beginTransaction());
        await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['prepared_row1']));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(?)', [10]);

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        expect(fn () => await($slowPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $client->close();
    });

    it('allows further inserts and commit after cancelling a prepared query inside a transaction', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['prepared_row1']));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(?)', [10]);

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        try {
            await($slowPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['after_cancel']));
            await($tx->commit());
        }

        $result = await($client->query('SELECT COUNT(*) as count FROM client_cancel_test'));
        $count = (int) $result->fetchOne()['count'];
        expect($count)->toBeGreaterThanOrEqual(2);

        $client->close();
    });

    it('persists only the pre-cancel and post-cancel rows after commit following prepared query cancellation', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['prepared_row1']));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(?)', [10]);

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        try {
            await($slowPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['after_cancel']));
            await($tx->commit());
        }

        $rows = await($client->query('SELECT value FROM client_cancel_test ORDER BY id'));
        $values = array_column($rows->fetchAll(), 'value');

        expect($values)->toContain('prepared_row1')
            ->and($values)->toContain('after_cancel')
            ->and($values)->not->toContain('')
        ;

        $client->close();
    });

    test('pool connection is healthy after cancellation and commit of a prepared transaction query', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['prepared_row1']));

        $slowPromise = $tx->query('INSERT INTO client_cancel_test (value) SELECT SLEEP(?)', [10]);

        Loop::addTimer(2.0, function () use ($slowPromise): void {
            $slowPromise->cancel();
        });

        try {
            await($slowPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->query('INSERT INTO client_cancel_test (value) VALUES (?)', ['after_cancel']));
            await($tx->commit());
        }

        $result = await($client->query('SELECT "Alive" AS status'));
        expect($result->fetchOne()['status'])->toBe('Alive');

        $client->close();
    });
});

describe('Transaction Stream Cancellation', function (): void {

    it('cancels a streaming SELECT inside a transaction and throws CancelledException or QueryException', function (): void {
        $client = makeTransactionClient();
        $startTime = microtime(true);

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('stream1'), ('stream2'), ('stream3')"));

        $streamPromise = $tx->stream('SELECT value, SLEEP(10) as delay FROM client_cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $client->close();
    });

    it('allows a query inside the transaction after cancelling a stream', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('stream1'), ('stream2'), ('stream3')"));

        $streamPromise = $tx->stream('SELECT value, SLEEP(10) as delay FROM client_cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        try {
            await($streamPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            $result = await($tx->query('SELECT COUNT(*) as count FROM client_cancel_test'));
            $txCount = (int) $result->fetchOne()['count'];
            expect($txCount)->toBeGreaterThanOrEqual(3);

            await($tx->rollback());
        }

        $client->close();
    });

    it('discards all rows after rollback following a cancelled stream', function (): void {
        $client = makeTransactionClient();

        // Pre-seed rows from a prior committed transaction to verify only those survive
        $setup = await($client->beginTransaction());
        await($setup->query("INSERT INTO client_cancel_test (value) VALUES ('committed_row1'), ('committed_row2')"));
        await($setup->commit());

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('stream1'), ('stream2'), ('stream3')"));

        $streamPromise = $tx->stream('SELECT value, SLEEP(10) as delay FROM client_cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        try {
            await($streamPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->rollback());
        }

        $result = await($client->query('SELECT COUNT(*) as count FROM client_cancel_test'));
        $count = (int) $result->fetchOne()['count'];

        expect($count)->toBe(2);

        $client->close();
    });

    test('pool connection is healthy after a cancelled stream and rollback', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('stream1'), ('stream2'), ('stream3')"));

        $streamPromise = $tx->stream('SELECT value, SLEEP(10) as delay FROM client_cancel_test');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        try {
            await($streamPromise);
        } catch (CancelledException) {
            // expected
        }

        if ($tx->isActive()) {
            await($tx->rollback());
        }

        $result = await($client->query('SELECT "Alive" AS status'));
        expect($result->fetchOne()['status'])->toBe('Alive');

        $client->close();
    });
});

describe('Transaction Commit and Rollback Cancellation Resistance', function (): void {

    it('completes commit despite an immediate cancel() call and persists data', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('commit_test')"));

        $commitPromise = $tx->commit();
        $commitPromise->cancel();

        $committed = false;

        try {
            await($commitPromise);
            $committed = true;
        } catch (CancelledException) {
            // commit may surface as cancelled, but data should still be persisted
        }

        $result = await($client->query('SELECT COUNT(*) as count FROM client_cancel_test'));
        $count = (int) $result->fetchOne()['count'];

        // Either commit resolved normally OR it was cancelled but still flushed to disk
        expect($count)->toBeGreaterThanOrEqual(1);

        $client->close();
    });

    it('completes rollback despite an immediate cancel() call and discards data', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('rollback_test')"));

        $rollbackPromise = $tx->rollback();
        $rollbackPromise->cancel();

        try {
            await($rollbackPromise);
        } catch (CancelledException) {
            // rollback may surface as cancelled
        }

        $result = await($client->query('SELECT COUNT(*) as count FROM client_cancel_test'));
        $count = (int) $result->fetchOne()['count'];

        expect($count)->toBe(0);

        $client->close();
    });

    test('pool connection is healthy after a cancelled commit', function (): void {
        $client = makeTransactionClient();

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO client_cancel_test (value) VALUES ('commit_test')"));

        $commitPromise = $tx->commit();
        $commitPromise->cancel();

        try {
            await($commitPromise);
        } catch (CancelledException) {
            // expected
        }

        $result = await($client->query('SELECT "Alive" AS status'));
        expect($result->fetchOne()['status'])->toBe('Alive');

        $client->close();
    });
});
