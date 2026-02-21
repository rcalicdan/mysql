<?php

declare(strict_types=1);

use function Hibla\async;
use function Hibla\await;
use function Hibla\delay;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Internals\RowStream;
use Hibla\Promise\Exceptions\CancelledException;

describe('Query Cancellation', function (): void {

    it('cancels a long-running query and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $conn->close();
    });

    it('marks the connection as cancelled after a query is cancelled', function (): void {
        $conn = makeConnection();

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        expect($conn->wasQueryCancelled())->toBeTrue();

        $conn->close();
    });

    it('can clear the cancelled flag after cancellation', function (): void {
        $conn = makeConnection();

        $queryPromise = $conn->query('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn () => await($queryPromise))
            ->toThrow(CancelledException::class)
        ;

        $conn->clearCancelledFlag();

        expect($conn->wasQueryCancelled())->toBeFalse();

        $conn->close();
    });
});

describe('Prepared Statement Cancellation', function (): void {

    it('cancels a long-running prepared statement execution', function (): void {
        $conn = makeConnection();
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
        $conn->close();
    });

    it('marks the connection as cancelled after a prepared statement is cancelled', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $execPromise = $stmt->execute([10]);

        Loop::addTimer(2.0, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn () => await($execPromise))
            ->toThrow(CancelledException::class)
        ;

        expect($conn->wasQueryCancelled())->toBeTrue();

        await($stmt->close());
        $conn->close();
    });

    it('recovers and reuses the connection after a prepared statement is cancelled', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $execPromise = $stmt->execute([10]);

        Loop::addTimer(2.0, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn () => await($execPromise))
            ->toThrow(CancelledException::class)
        ;

        if ($conn->wasQueryCancelled()) {
            await($conn->query('DO SLEEP(0)'));
            $conn->clearCancelledFlag();
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
        ;

        await($stmt->close());
        $conn->close();
    });
});

describe('Stream Query Cancellation', function (): void {
    it('cancels a streaming query before the first row arrives and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        $streamPromise = $conn->streamQuery('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        $conn->close();
    });

    it('marks the connection as cancelled after a stream is cancelled before first row', function (): void {
        $conn = makeConnection();

        $streamPromise = $conn->streamQuery('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        expect($conn->wasQueryCancelled())->toBeTrue();

        $conn->close();
    });

    it('recovers and reuses the connection after a stream is cancelled before first row', function (): void {
        $conn = makeConnection();

        $streamPromise = $conn->streamQuery('SELECT SLEEP(10)');

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        if ($conn->wasQueryCancelled()) {
            await($conn->query('DO SLEEP(0)'));
            $conn->clearCancelledFlag();
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
        ;

        $conn->close();
    });

    it('cancels mid-iteration via break and stream reports as cancelled', function (): void {
        $conn = makeConnection();

        $stream = await($conn->streamQuery(twentyRowSql()));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;

        foreach ($stream as $row) {
            $rowsReceived++;

            if ($rowsReceived >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($rowsReceived)->toBe(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        $conn->close();
    });

    test('wasQueryCancelled is false when cancelling a fully-buffered stream mid-iteration', function (): void {
        $conn = makeConnection();

        $stream = await($conn->streamQuery(twentyRowSql()));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        expect($conn->wasQueryCancelled())->toBeFalse();

        $conn->close();
    });

    test('connection remains healthy after cancelling a fully-buffered stream mid-iteration', function (): void {
        $conn = makeConnection();

        $stream = await($conn->streamQuery(twentyRowSql()));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $conn->close();
    });

    it('cancels mid-iteration via async timer between rows and throws CancelledException', function (): void {
        $conn = makeConnection();

        $stream = await($conn->streamQuery(twentyRowSql()));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;
        $cancelFired = false;

        expect(function () use ($stream, &$rowsReceived, &$cancelFired): void {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        })->toThrow(CancelledException::class);

        expect($rowsReceived)->toBeGreaterThanOrEqual(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        $conn->close();
    });

    test('connection remains healthy after cancelling a stream via async timer between rows', function (): void {
        $conn = makeConnection();

        $stream = await($conn->streamQuery(twentyRowSql()));

        $rowsReceived = 0;
        $cancelFired = false;

        try {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        } catch (CancelledException) {
            // expected — proceed to health check
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $conn->close();
    });
});

describe('Execute Stream Cancellation', function (): void {
    it('cancels executeStream before the first row arrives and throws CancelledException', function (): void {
        $conn = makeConnection();
        $startTime = microtime(true);

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $streamPromise = $stmt->executeStream([10]);

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        await($stmt->close());
        $conn->close();
    });

    it('marks the connection as cancelled after executeStream is cancelled before first row', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $streamPromise = $stmt->executeStream([10]);

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        expect($conn->wasQueryCancelled())->toBeTrue();

        await($stmt->close());
        $conn->close();
    });

    it('recovers and reuses the connection after executeStream is cancelled before first row', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $streamPromise = $stmt->executeStream([10]);

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        if ($conn->wasQueryCancelled()) {
            await($conn->query('DO SLEEP(0)'));
            $conn->clearCancelledFlag();
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
        ;

        await($stmt->close());
        $conn->close();
    });

    it('cancels executeStream mid-iteration via break and stream reports as cancelled', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare(twentyRowPreparedSql()));
        $stream = await($stmt->executeStream([]));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;

        foreach ($stream as $row) {
            $rowsReceived++;

            if ($rowsReceived >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($rowsReceived)->toBe(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        await($stmt->close());
        $conn->close();
    });

    test('wasQueryCancelled is false when cancelling a fully-buffered executeStream mid-iteration', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare(twentyRowPreparedSql()));
        $stream = await($stmt->executeStream([]));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        expect($conn->wasQueryCancelled())->toBeFalse();

        await($stmt->close());
        $conn->close();
    });

    test('connection remains healthy after cancelling a fully-buffered executeStream mid-iteration', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare(twentyRowPreparedSql()));
        $stream = await($stmt->executeStream([]));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive');

        await($stmt->close());
        $conn->close();
    });

    it('cancels executeStream mid-iteration via async timer between rows and throws CancelledException', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare(twentyRowPreparedSql()));
        $stream = await($stmt->executeStream([]));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;
        $cancelFired = false;

        expect(function () use ($stream, &$rowsReceived, &$cancelFired): void {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        })->toThrow(CancelledException::class);

        expect($rowsReceived)->toBeGreaterThanOrEqual(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        await($stmt->close());
        $conn->close();
    });

    test('connection remains healthy after cancelling executeStream via async timer between rows', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare(twentyRowPreparedSql()));
        $stream = await($stmt->executeStream([]));

        $rowsReceived = 0;
        $cancelFired = false;

        try {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        } catch (CancelledException) {
            // expected — proceed to health check
        }

        $result = await($conn->query('SELECT "Alive" AS status'));

        expect($result->fetchOne()['status'])->toBe('Alive');

        await($stmt->close());
        $conn->close();
    });

    it('can reuse a prepared statement after executeStream is cancelled', function (): void {
        $conn = makeConnection();

        $stmt = await($conn->prepare('SELECT SLEEP(?)'));
        $streamPromise = $stmt->executeStream([5]);

        Loop::addTimer(1.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class)
        ;

        if ($conn->wasQueryCancelled()) {
            await($conn->query('DO SLEEP(0)'));
            $conn->clearCancelledFlag();
        }

        $verifyStmt = await($conn->prepare('SELECT ? AS result'));
        $stream = await($verifyStmt->executeStream([42]));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(1)
            ->and($rows[0]['result'])->toBe(42)
        ;

        await($stmt->close());
        await($verifyStmt->close());
        $conn->close();
    });

    test('connection is fully healthy after all executeStream cancellation scenarios', function (): void {
        $conn = makeConnection();

        $result = await($conn->query('SELECT "AllGreen" AS status'));

        expect($result->fetchOne()['status'])->toBe('AllGreen');

        $conn->close();
    });
});
