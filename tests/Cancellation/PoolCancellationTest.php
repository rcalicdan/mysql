<?php

declare(strict_types=1);

use function Hibla\await;
use function Hibla\delay;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Internals\Connection;
use Hibla\Promise\Exceptions\CancelledException;

describe('Pool Waiter Cancellation', function (): void {

    it('throws CancelledException when awaiting a cancelled waiter', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $waiter = $pool->get();
        $waiter->cancel();

        expect(fn () => await($waiter))
            ->toThrow(CancelledException::class)
        ;

        $pool->release($conn);
        $pool->close();
    });

    it('does not decrement active connections when a waiter is cancelled', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        expect($pool->getStats()['active_connections'])->toBe(1);

        $waiter = $pool->get();
        $waiter->cancel();

        expect($pool->getStats()['active_connections'])->toBe(1);

        $pool->release($conn);
        $pool->close();
    });

    it('skips a cancelled waiter and resolves the next active waiter on release', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $cancelledWaiter = $pool->get();
        $activeWaiter = $pool->get();

        expect($pool->getStats()['waiting_requests'])->toBe(2);

        $cancelledWaiter->cancel();

        // Lazy cleanup — cancelled waiter is still in the queue until dequeue
        expect($pool->getStats()['waiting_requests'])->toBe(2);

        $pool->release($conn);

        $resolvedConn = await($activeWaiter);

        expect($resolvedConn)->toBeInstanceOf(Connection::class)
            ->and($resolvedConn->isReady())->toBeTrue()
            ->and($pool->getStats()['waiting_requests'])->toBe(0)
        ;

        $pool->release($resolvedConn);
        $pool->close();
    });

    it('returns the connection to the pool when released after skipping a cancelled waiter', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $waiter = $pool->get();
        $waiter->cancel();

        $pool->release($conn);

        // No active waiters — connection should park in idle pool
        expect($pool->getStats()['pooled_connections'])->toBe(1);

        $pool->close();
    });

    it('handles multiple cancelled waiters and resolves the first active one', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $w1 = $pool->get();
        $w2 = $pool->get();
        $w3 = $pool->get();

        $w1->cancel();
        $w2->cancel();

        $pool->release($conn);

        $resolvedConn = await($w3);

        expect($resolvedConn)->toBeInstanceOf(Connection::class)
            ->and($resolvedConn->isReady())->toBeTrue()
        ;

        $pool->release($resolvedConn);
        $pool->close();
    });
});

describe('Pool Drain and Release', function (): void {

    it('absorbs stale kill flag via DO SLEEP(0) before returning connection to pool', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        expect($conn->wasQueryCancelled())->toBeTrue();

        // Release triggers drainAndRelease internally
        $pool->release($conn);

        // Wait long enough for the kill connection to open, execute, and drain
        await(delay(3.0));

        expect($pool->getStats()['pooled_connections'])->toBe(1)
            ->and($pool->getStats()['draining_connections'])->toBe(0)
        ;

        $pool->close();
    });

    it('connection is healthy and reusable after pool drains the stale kill flag', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        $pool->release($conn);

        // Wait for drain to complete
        await(delay(3.0));

        $reusedConn = await($pool->get());
        $result = await($reusedConn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($reusedConn->wasQueryCancelled())->toBeFalse()
        ;

        $pool->release($reusedConn);
        $pool->close();
    });

    it('pool does not expose a cancelled connection to the next waiter before drain completes', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        // Queue a waiter before releasing
        $waiter = $pool->get();

        // Release — triggers drainAndRelease, not immediate hand-off
        $pool->release($conn);

        $nextConn = await($waiter);

        // By the time the waiter resolves the drain must have completed
        expect($nextConn->wasQueryCancelled())->toBeFalse()
            ->and($nextConn->isReady())->toBeTrue()
        ;

        $result = await($nextConn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $pool->release($nextConn);
        $pool->close();
    });

    test('draining connections are closed safely when pool is closed mid-drain', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        $pool->release($conn);

        // Close the pool immediately — drain is still in progress
        $pool->close();

        expect($pool->getStats()['draining_connections'])->toBe(0)
            ->and($pool->getStats()['active_connections'])->toBe(0)
        ;
    });
});

describe('Pool Query Cancellation Integration', function (): void {

    it('cancels a plain query obtained via the pool and throws CancelledException', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());
        $startTime = microtime(true);

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $pool->release($conn);
        $pool->close();
    });

    it('cancels a prepared statement obtained via the pool and throws CancelledException', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());
        $startTime = microtime(true);

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $execPromise = $stmt->execute([10]);

        Loop::addTimer(2.0, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn () => await($execPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        await($stmt->close());
        $pool->release($conn);
        $pool->close();
    });

    it('cancels a stream query obtained via the pool and throws CancelledException', function (): void {
        $pool = makePool(1);
        $conn = await($pool->get());
        $startTime = microtime(true);

        $streamPromise = $conn->streamQuery('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $pool->release($conn);
        $pool->close();
    });

    test('pool stats are consistent after a cancelled query is released and drained', function (): void {
        $pool = makePool(2);
        $conn1 = await($pool->get());
        $conn2 = await($pool->get());

        $queryPromise = $conn1->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        $pool->release($conn1);
        $pool->release($conn2);

        await(delay(3.0));

        $stats = $pool->getStats();

        expect($stats['pooled_connections'])->toBe(2)
            ->and($stats['active_connections'])->toBe(2)
            ->and($stats['draining_connections'])->toBe(0)
            ->and($stats['waiting_requests'])->toBe(0)
        ;

        $pool->close();
    });

    test('pool remains fully operational after multiple cancellations across connections', function (): void {
        $pool = makePool(3);

        for ($i = 0; $i < 3; $i++) {
            $conn = await($pool->get());
            $queryPromise = $conn->query('SELECT SLEEP(10)');

            Loop::addTimer(1.0, function () use ($queryPromise): void {
                $queryPromise->cancel();
            });

            expect(fn () => await($queryPromise))
                ->toThrow(CancelledException::class)
            ;

            $pool->release($conn);
        }

        await(delay(3.0));

        $conn = await($pool->get());
        $result = await($conn->query('SELECT "AllGreen" AS status'));

        expect($result->fetchOne()['status'])->toBe('AllGreen')
            ->and($pool->getStats()['draining_connections'])->toBe(0)
        ;

        $pool->release($conn);
        $pool->close();
    });
});
