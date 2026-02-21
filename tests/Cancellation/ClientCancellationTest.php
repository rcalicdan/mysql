<?php

declare(strict_types=1);

use function Hibla\await;
use function Hibla\delay;

use Hibla\EventLoop\Loop;
use Hibla\Promise\Exceptions\CancelledException;

describe('Client Query Cancellation', function (): void {

    // =========================================================================
    // Scenario 1: Cancel non-prepared query (Text Protocol)
    // =========================================================================

    it('cancels a non-prepared query via the client and throws CancelledException', function (): void {
        $client = makeClient();
        $startTime = microtime(true);

        $queryPromise = $client->query('SELECT SLEEP(10) AS result');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $client->close();
    });

    test('pool connection is healthy after a cancelled non-prepared query', function (): void {
        $client = makeClient();

        $queryPromise = $client->query('SELECT SLEEP(10) AS result');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        $result = await($client->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $client->close();
    });

    // =========================================================================
    // Scenario 2: Cancel prepared query (Binary Protocol)
    // =========================================================================

    it('cancels a prepared query via the client and throws CancelledException', function (): void {
        $client = makeClient();
        $startTime = microtime(true);

        $queryPromise = $client->query('SELECT SLEEP(?) AS result', [10]);

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $client->close();
    });

    test('pool connection is healthy after a cancelled prepared query', function (): void {
        $client = makeClient();

        $queryPromise = $client->query('SELECT SLEEP(?) AS result', [10]);

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        $result = await($client->query('SELECT ? AS echo_value', ['HelloAfterCancel']));

        expect($result->fetchOne()['echo_value'])->toBe('HelloAfterCancel');

        $client->close();
    });
});

describe('Client Waiter Cancellation', function (): void {

    // =========================================================================
    // Scenario 3: Cancel while waiting for a pool connection (pre-server)
    // =========================================================================
    // The pool is fully saturated so the query never reaches the server.
    // Cancelling the waiter promise should not dispatch KILL QUERY and should
    // not affect pool accounting.
    // =========================================================================

    it('cancels a queued waiter before it reaches the server and throws CancelledException', function (): void {
        $client = makeClient(maxConnections: 5);

        // Saturate the pool
        $holders = [];
        for ($i = 0; $i < 5; $i++) {
            $holders[] = $client->query('SELECT SLEEP(10)');
        }

        $waiterPromise = $client->query('SELECT "ShouldNotRun" AS v');

        Loop::addTimer(0.5, function () use ($waiterPromise): void {
            $waiterPromise->cancel();
        });

        expect(fn () => await($waiterPromise))
            ->toThrow(CancelledException::class)
        ;

        // Cancel holders to clean up
        foreach ($holders as $holder) {
            $holder->cancel();
        }

        // Allow pool to drain and absorb kill flags
        await(delay(3.0));

        $client->close();
    });

    test('pool remains functional after a cancelled waiter', function (): void {
        $client = makeClient(maxConnections: 5);

        $holders = [];
        for ($i = 0; $i < 5; $i++) {
            $holders[] = $client->query('SELECT SLEEP(10)');
        }

        $waiterPromise = $client->query('SELECT "ShouldNotRun" AS v');

        Loop::addTimer(0.5, function () use ($waiterPromise): void {
            $waiterPromise->cancel();
        });

        expect(fn () => await($waiterPromise))
            ->toThrow(CancelledException::class)
        ;

        foreach ($holders as $holder) {
            $holder->cancel();
        }

        await(delay(3.0));

        $result = await($client->query('SELECT "PoolOk" AS status'));

        expect($result->fetchOne()['status'])->toBe('PoolOk');

        $client->close();
    });

    test('pool stats show no draining connections after waiter cancellation and drain', function (): void {
        $client = makeClient(maxConnections: 5);

        $holders = [];
        for ($i = 0; $i < 5; $i++) {
            $holders[] = $client->query('SELECT SLEEP(10)');
        }

        $waiterPromise = $client->query('SELECT "ShouldNotRun" AS v');

        Loop::addTimer(0.5, function () use ($waiterPromise): void {
            $waiterPromise->cancel();
        });

        expect(fn () => await($waiterPromise))
            ->toThrow(CancelledException::class)
        ;

        foreach ($holders as $holder) {
            $holder->cancel();
        }

        await(delay(3.0));

        $stats = $client->getStats();

        expect($stats['draining_connections'])->toBe(0)
            ->and($stats['waiting_requests'])->toBe(0)
        ;

        $client->close();
    });
});
