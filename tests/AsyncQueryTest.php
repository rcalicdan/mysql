<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\Promise\Promise;

beforeAll(function (): void {
    $client = makeConcurrentClient();

    await($client->query('CREATE TABLE IF NOT EXISTS concurrent_test (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        worker     INT,
        value      VARCHAR(100),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )'));

    $client->close();
});

afterAll(function (): void {
    $client = makeConcurrentClient();
    await($client->query('DROP TABLE IF EXISTS concurrent_test'));
    $client->close();
});

beforeEach(function (): void {
    $client = makeConcurrentClient();
    await($client->query('TRUNCATE TABLE concurrent_test'));
    $client->close();
});

describe('Concurrent Query Execution', function (): void {

    describe('Parallel SELECT', function (): void {

        it('executes multiple SELECT queries concurrently and all resolve', function (): void {
            $client = makeConcurrentClient();

            $promises = [];
            for ($i = 1; $i <= 5; $i++) {
                $promises[] = $client->query("SELECT {$i} AS val");
            }

            $results = await(Promise::all($promises));

            expect($results)->toHaveCount(5);

            foreach ($results as $index => $result) {
                expect($result->fetchOne()['val'])->toBe((string) ($index + 1));
            }

            $client->close();
        });

        it('concurrent queries complete faster than sequential would', function (): void {
            $client = makeConcurrentClient(maxConnections: 5);

            $promises = [];
            for ($i = 0; $i < 5; $i++) {
                $promises[] = $client->query('SELECT SLEEP(0.1) AS done');
            }

            $start = microtime(true);
            $results = await(Promise::all($promises));
            $elapsed = microtime(true) - $start;

            expect($results)->toHaveCount(5)
                ->and($elapsed)->toBeLessThan(0.4)
            ;

            $client->close();
        });

        it('all concurrent queries return correct independent results', function (): void {
            $client = makeConcurrentClient();

            $promises = [
                $client->query('SELECT 100 AS val'),
                $client->query('SELECT 200 AS val'),
                $client->query('SELECT 300 AS val'),
            ];

            [$r1, $r2, $r3] = await(Promise::all($promises));

            expect($r1->fetchOne()['val'])->toBe('100')
                ->and($r2->fetchOne()['val'])->toBe('200')
                ->and($r3->fetchOne()['val'])->toBe('300')
            ;

            $client->close();
        });

        it('runs 10 concurrent queries without exhausting the pool', function (): void {
            $client = makeConcurrentClient(maxConnections: 10);

            $promises = [];
            for ($i = 1; $i <= 10; $i++) {
                $promises[] = $client->query("SELECT {$i} AS val");
            }

            $results = await(Promise::all($promises));

            expect($results)->toHaveCount(10);

            $client->close();
        });

        it('queues excess requests when concurrent count exceeds pool size', function (): void {
            $client = makeConcurrentClient(maxConnections: 3);

            $promises = [];
            for ($i = 1; $i <= 6; $i++) {
                $promises[] = $client->query("SELECT {$i} AS val");
            }

            $results = await(Promise::all($promises));

            expect($results)->toHaveCount(6);

            foreach ($results as $index => $result) {
                expect($result->fetchOne()['val'])->toBe((string) ($index + 1));
            }

            $client->close();
        });
    });

    describe('Parallel INSERT', function (): void {

        it('executes multiple INSERTs concurrently and all persist', function (): void {
            $client = makeConcurrentClient();

            $promises = [];
            for ($i = 1; $i <= 5; $i++) {
                $promises[] = $client->execute(
                    'INSERT INTO concurrent_test (worker, value) VALUES (?, ?)',
                    [$i, "worker_{$i}"]
                );
            }

            $results = await(Promise::all($promises));

            expect($results)->toHaveCount(5)
                ->and(array_sum($results))->toBe(5)
            ;

            $count = await($client->query('SELECT COUNT(*) AS cnt FROM concurrent_test'));

            expect((int) $count->fetchOne()['cnt'])->toBe(5);

            $client->close();
        });

        it('concurrent INSERTs produce unique auto-increment IDs', function (): void {
            $client = makeConcurrentClient();

            $promises = [];
            for ($i = 1; $i <= 5; $i++) {
                $promises[] = $client->executeGetId(
                    'INSERT INTO concurrent_test (worker, value) VALUES (?, ?)',
                    [$i, "id_check_{$i}"]
                );
            }

            $ids = await(Promise::all($promises));

            expect(array_unique($ids))->toHaveCount(5);

            $client->close();
        });

        it('concurrent INSERTs each report correct affected row count', function (): void {
            $client = makeConcurrentClient();

            $promises = [];
            for ($i = 1; $i <= 4; $i++) {
                $promises[] = $client->execute(
                    'INSERT INTO concurrent_test (worker, value) VALUES (?, ?)',
                    [$i, "affected_{$i}"]
                );
            }

            $affected = await(Promise::all($promises));

            foreach ($affected as $rows) {
                expect($rows)->toBe(1);
            }

            $client->close();
        });
    });

    describe('Parallel Mixed Operations', function (): void {

        it('runs concurrent SELECT and INSERT operations together', function (): void {
            $client = makeConcurrentClient();

            $promises = [
                $client->query('SELECT 1 AS val'),
                $client->execute("INSERT INTO concurrent_test (worker, value) VALUES (1, 'mixed')"),
                $client->query('SELECT 2 AS val'),
                $client->execute("INSERT INTO concurrent_test (worker, value) VALUES (2, 'mixed')"),
            ];

            [$select1, $insert1, $select2, $insert2] = await(Promise::all($promises));

            expect($select1->fetchOne()['val'])->toBe('1')
                ->and($insert1)->toBe(1)
                ->and($select2->fetchOne()['val'])->toBe('2')
                ->and($insert2)->toBe(1)
            ;

            $client->close();
        });

        it('concurrent fetchOne calls return independent results', function (): void {
            $client = makeConcurrentClient();

            $promises = [
                $client->fetchOne('SELECT 10 AS val'),
                $client->fetchOne('SELECT 20 AS val'),
                $client->fetchOne('SELECT 30 AS val'),
            ];

            [$r1, $r2, $r3] = await(Promise::all($promises));

            expect($r1['val'])->toBe('10')
                ->and($r2['val'])->toBe('20')
                ->and($r3['val'])->toBe('30')
            ;

            $client->close();
        });

        it('concurrent fetchValue calls return correct values', function (): void {
            $client = makeConcurrentClient();

            $promises = [
                $client->fetchValue('SELECT 111 AS val', 'val'),
                $client->fetchValue('SELECT 222 AS val', 'val'),
                $client->fetchValue('SELECT 333 AS val', 'val'),
            ];

            [$v1, $v2, $v3] = await(Promise::all($promises));

            expect($v1)->toBe('111')
                ->and($v2)->toBe('222')
                ->and($v3)->toBe('333')
            ;

            $client->close();
        });

        it('mixes prepared statements and plain queries concurrently', function (): void {
            $client = makeConcurrentClient();

            $stmt = await($client->prepare('SELECT ? AS val'));

            $promises = [
                $client->query('SELECT 1 AS val'),
                $stmt->execute([2]),
                $client->query('SELECT 3 AS val'),
                $stmt->execute([4]),
            ];

            $results = await(Promise::all($promises));

            expect($results[0]->fetchOne()['val'])->toBe('1')
                ->and($results[1]->fetchOne()['val'])->toBe(2)
                ->and($results[2]->fetchOne()['val'])->toBe('3')
                ->and($results[3]->fetchOne()['val'])->toBe(4)
            ;

            await($stmt->close());
            $client->close();
        });
    });

    describe('Parallel Streams', function (): void {

        it('opens multiple streams concurrently and all yield rows', function (): void {
            $client = makeConcurrentClient();

            for ($i = 1; $i <= 3; $i++) {
                await($client->execute(
                    'INSERT INTO concurrent_test (worker, value) VALUES (?, ?)',
                    [$i, "stream_{$i}"]
                ));
            }

            $promises = [
                $client->stream('SELECT value FROM concurrent_test WHERE worker = 1'),
                $client->stream('SELECT value FROM concurrent_test WHERE worker = 2'),
                $client->stream('SELECT value FROM concurrent_test WHERE worker = 3'),
            ];

            [$s1, $s2, $s3] = await(Promise::all($promises));

            $collect = function ($stream): array {
                $rows = [];
                foreach ($stream as $row) {
                    $rows[] = $row;
                }

                return $rows;
            };

            expect($collect($s1))->toHaveCount(1)
                ->and($collect($s2))->toHaveCount(1)
                ->and($collect($s3))->toHaveCount(1)
            ;

            $client->close();
        });

        it('concurrent streams consume independently without interference', function (): void {
            $client = makeConcurrentClient();

            $promises = [
                $client->stream('SELECT 1 AS val UNION SELECT 2 UNION SELECT 3'),
                $client->stream('SELECT 4 AS val UNION SELECT 5 UNION SELECT 6'),
            ];

            [$s1, $s2] = await(Promise::all($promises));

            $rows1 = [];
            foreach ($s1 as $row) {
                $rows1[] = $row['val'];
            }

            $rows2 = [];
            foreach ($s2 as $row) {
                $rows2[] = $row['val'];
            }

            expect($rows1)->toBe(['1', '2', '3'])
                ->and($rows2)->toBe(['4', '5', '6'])
            ;

            $client->close();
        });
    });

    describe('Parallel Transactions', function (): void {

        it('runs multiple transactions concurrently and all commit', function (): void {
            $client = makeConcurrentClient();

            $promises = [];
            for ($i = 1; $i <= 3; $i++) {
                $promises[] = $client->transaction(function ($tx) use ($i) {
                    $result = await($tx->execute(
                        'INSERT INTO concurrent_test (worker, value) VALUES (?, ?)',
                        [$i, "tx_{$i}"]
                    ));

                    return $result;
                });
            }

            $results = await(Promise::all($promises));

            expect(array_sum($results))->toBe(3);

            $count = await($client->query('SELECT COUNT(*) AS cnt FROM concurrent_test'));

            expect((int) $count->fetchOne()['cnt'])->toBe(3);

            $client->close();
        });

        it('one transaction failing does not affect others in Promise::allSettled', function (): void {
            $client = makeConcurrentClient();

            $promises = [
                $client->transaction(function ($tx) {
                    return await($tx->execute(
                        "INSERT INTO concurrent_test (worker, value) VALUES (1, 'good_1')"
                    ));
                }),
                $client->transaction(function (): void {
                    throw new RuntimeException('intentional failure');
                }),
                $client->transaction(function ($tx) {
                    return await($tx->execute(
                        "INSERT INTO concurrent_test (worker, value) VALUES (3, 'good_3')"
                    ));
                }),
            ];

            $results = await(Promise::allSettled($promises));

            $fulfilled = 0;
            $rejected = 0;

            foreach ($results as $result) {
                if ($result->isFulfilled()) {
                    $fulfilled++;
                } elseif ($result->isRejected()) {
                    $rejected++;
                }
            }

            expect($fulfilled)->toBe(2);
            expect($rejected)->toBe(1);

            $count = await($client->query('SELECT COUNT(*) AS cnt FROM concurrent_test'));

            expect((int) $count->fetchOne()['cnt'])->toBe(2);

            $client->close();
        });

        it('concurrent transactions on the same table do not corrupt data', function (): void {
            $client = makeConcurrentClient();

            $promises = [];
            for ($i = 1; $i <= 5; $i++) {
                $promises[] = $client->transaction(function ($tx) use ($i) {
                    await($tx->execute(
                        'INSERT INTO concurrent_test (worker, value) VALUES (?, ?)',
                        [$i, "integrity_{$i}"]
                    ));
                    $result = await($tx->query('SELECT COUNT(*) AS cnt FROM concurrent_test'));
                    $cnt = (int) $result->fetchOne()['cnt'];

                    return $cnt;
                });
            }

            await(Promise::all($promises));

            $final = await($client->query('SELECT COUNT(*) AS cnt FROM concurrent_test'));

            expect((int) $final->fetchOne()['cnt'])->toBe(5);

            $client->close();
        });
    });

    describe('Pool Behavior Under Concurrent Load', function (): void {

        it('all promises resolve even when pool must queue excess requests', function (): void {
            $client = makeConcurrentClient(maxConnections: 3);

            $promises = [];
            for ($i = 1; $i <= 10; $i++) {
                $promises[] = $client->query("SELECT {$i} AS val, SLEEP(0.05) AS done");
            }

            $start = microtime(true);
            $results = await(Promise::all($promises));
            $elapsed = microtime(true) - $start;

            expect($results)->toHaveCount(10);

            foreach ($results as $index => $result) {
                expect($result->fetchOne()['val'])->toBe((string) ($index + 1));
            }

            $client->close();
        });

        it('pool stats show no leaked connections after concurrent load', function (): void {
            $client = makeConcurrentClient(maxConnections: 5);

            $promises = [];
            for ($i = 1; $i <= 10; $i++) {
                $promises[] = $client->query("SELECT {$i} AS val");
            }

            await(Promise::all($promises));

            $stats = $client->getStats();

            expect($stats['waiting_requests'])->toBe(0)
                ->and($stats['pooled_connections'])->toBe($stats['active_connections'])
            ;

            $client->close();
        });

        it('pool recovers fully after a burst of concurrent failures', function (): void {
            $client = makeConcurrentClient(maxConnections: 5);

            $promises = [];
            for ($i = 0; $i < 5; $i++) {
                $promises[] = $client->query('SELECT * FROM this_table_does_not_exist');
            }

            try {
                await(Promise::all($promises));
            } catch (Throwable $e) {
                // Expected failure
            }

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('concurrent health checks do not interfere with active queries', function (): void {
            $client = makeConcurrentClient(maxConnections: 5);

            await($client->query('SELECT 1'));

            $promises = [
                $client->healthCheck(),
                $client->query('SELECT 2 AS val'),
                $client->query('SELECT 3 AS val'),
            ];

            [$healthStats, $r1, $r2] = await(Promise::all($promises));

            expect($healthStats['unhealthy'])->toBe(0)
                ->and($r1->fetchOne()['val'])->toBe('2')
                ->and($r2->fetchOne()['val'])->toBe('3')
            ;

            $client->close();
        });
    });
});
