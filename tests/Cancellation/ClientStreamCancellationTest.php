<?php

declare(strict_types=1);

use function Hibla\await;
use function Hibla\delay;

use Hibla\EventLoop\Loop;
use Hibla\Promise\Exceptions\CancelledException;


function setupTable(mixed $client): void
{
    await($client->query('DROP TABLE IF EXISTS client_stream_test'));
    await($client->query('
        CREATE TABLE client_stream_test (
            id    INT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB
    '));

    $rows = [];
    for ($i = 1; $i <= 1000; $i++) {
        $rows[] = "('row_{$i}')";
    }
    await($client->query(
        'INSERT INTO client_stream_test (value) VALUES ' . implode(', ', $rows)
    ));
}

function teardownTable(mixed $client): void
{
    await($client->query('DROP TABLE IF EXISTS client_stream_test'));
}


describe('Client Stream Cancellation', function (): void {
    it('cancels a non-prepared stream before the first row arrives and throws CancelledException', function (): void {
        $client = makeClient();
        setupTable($client);

        $startTime = microtime(true);

        $streamPromise = $client->stream(
            'SELECT value, SLEEP(10) AS delay FROM client_stream_test'
        );

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        teardownTable($client);
        $client->close();
    });

    test('pool connection is healthy after a cancelled non-prepared stream', function (): void {
        $client = makeClient();
        setupTable($client);

        $streamPromise = $client->stream(
            'SELECT value, SLEEP(10) AS delay FROM client_stream_test'
        );

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class);

        await(delay(0.5));

        $result = await($client->query('SELECT "Alive" AS status'));
        expect($result->fetchOne()['status'])->toBe('Alive');

        teardownTable($client);
        $client->close();
    });

    it('cancels a prepared stream before the first row arrives and throws CancelledException', function (): void {
        $client = makeClient();
        setupTable($client);

        $startTime = microtime(true);

        $streamPromise = $client->stream(
            'SELECT value, SLEEP(?) AS delay FROM client_stream_test',
            [10]
        );

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(5.0);

        teardownTable($client);
        $client->close();
    });

    test('pool connection is healthy after a cancelled prepared stream', function (): void {
        $client = makeClient();
        setupTable($client);

        $streamPromise = $client->stream(
            'SELECT value, SLEEP(?) AS delay FROM client_stream_test',
            [10]
        );

        Loop::addTimer(2.0, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn () => await($streamPromise))
            ->toThrow(CancelledException::class);

        await(delay(0.5));

        $result = await($client->query('SELECT ? AS echo_val', ['StreamOk']));
        expect($result->fetchOne()['echo_val'])->toBe('StreamOk');

        teardownTable($client);
        $client->close();
    });

    it('cancels a stream mid-iteration after the first row and throws CancelledException', function (): void {
        $client = makeClient();
        setupTable($client);

        $stream   = await($client->stream('SELECT value FROM client_stream_test'));
        $rowsRead = 0;

        expect(function () use ($stream, &$rowsRead): void {
            foreach ($stream as $row) {
                $rowsRead++;
                if ($rowsRead === 1) {
                    $stream->cancel();
                }
            }
        })->toThrow(CancelledException::class);

        expect($rowsRead)->toBe(1);

        teardownTable($client);
        $client->close();
    });

    test('pool connection is healthy after a mid-iteration stream cancellation', function (): void {
        $client = makeClient();
        setupTable($client);

        $stream = await($client->stream('SELECT value FROM client_stream_test'));

        try {
            foreach ($stream as $row) {
                $stream->cancel();
            }
        } catch (CancelledException) {
            // expected
        }

        await(delay(0.5));

        $result = await($client->query('SELECT COUNT(*) AS count FROM client_stream_test'));
        expect((int) $result->fetchOne()['count'])->toBe(1000);

        teardownTable($client);
        $client->close();
    });

    test('cancelling an already-completed stream is a safe no-op', function (): void {
        $client = makeClient();
        setupTable($client);

        $stream   = await($client->stream('SELECT value FROM client_stream_test'));
        $rowsRead = 0;

        foreach ($stream as $row) {
            $rowsRead++;
        }

        $stream->cancel();

        expect($rowsRead)->toBe(1000);

        teardownTable($client);
        $client->close();
    });

    test('pool connection is healthy after cancelling an already-completed stream', function (): void {
        $client = makeClient();
        setupTable($client);

        $stream = await($client->stream('SELECT value FROM client_stream_test'));

        foreach ($stream as $row) {
            // consume all
        }

        $stream->cancel();

        $result = await($client->query('SELECT "StillAlive" AS status'));
        expect($result->fetchOne()['status'])->toBe('StillAlive');

        teardownTable($client);
        $client->close();
    });

    it('cancels multiple concurrent streams and all throw CancelledException', function (): void {
        $client = makeClient(maxConnections: 5);
        setupTable($client);

        $streamPromises = [];
        for ($i = 0; $i < 5; $i++) {
            $streamPromises[] = $client->stream(
                'SELECT value, SLEEP(10) AS delay FROM client_stream_test'
            );
        }

        Loop::addTimer(2.0, function () use ($streamPromises): void {
            foreach ($streamPromises as $p) {
                $p->cancel();
            }
        });

        $cancelled = 0;
        foreach ($streamPromises as $p) {
            try {
                await($p);
            } catch (CancelledException) {
                $cancelled++;
            }
        }

        expect($cancelled)->toBe(5);

        teardownTable($client);
        $client->close();
    });

    test('pool is fully functional after mass concurrent stream cancellation', function (): void {
        $client = makeClient(maxConnections: 5);
        setupTable($client);

        $streamPromises = [];
        for ($i = 0; $i < 5; $i++) {
            $streamPromises[] = $client->stream(
                'SELECT value, SLEEP(10) AS delay FROM client_stream_test'
            );
        }

        Loop::addTimer(2.0, function () use ($streamPromises): void {
            foreach ($streamPromises as $p) {
                $p->cancel();
            }
        });

        foreach ($streamPromises as $p) {
            try {
                await($p);
            } catch (CancelledException) {
                // expected
            }
        }

        await(delay(5.0));

        $result = await($client->query('SELECT "PoolRecovered" AS status'));

        expect($result->fetchOne()['status'])->toBe('PoolRecovered')
            ->and($client->getStats()['draining_connections'])->toBe(0);

        teardownTable($client);
        $client->close();
    });
});