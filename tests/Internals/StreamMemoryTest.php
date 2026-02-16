<?php

declare(strict_types=1);

use function Hibla\await;

beforeAll(function (): void {
    $conn = makeConnection();

    await($conn->query('CREATE TABLE IF NOT EXISTS stream_memory_test (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        uuid        VARCHAR(36),
        description TEXT,
        created_at  DATETIME
    )'));

    $batchSize = 2000;
    $totalRows = 100000;
    $batches = (int) ceil($totalRows / $batchSize);

    for ($i = 0; $i < $batches; $i++) {
        $values = [];
        for ($j = 0; $j < $batchSize; $j++) {
            $values[] = "('UUID-" . uniqid() . "', '" . str_repeat('X', 100) . "', NOW())";
        }
        await($conn->query(
            'INSERT INTO stream_memory_test (uuid, description, created_at) VALUES ' . implode(',', $values)
        ));
    }

    $conn->close();
});

afterAll(function (): void {
    $conn = makeConnection();
    await($conn->query('DROP TABLE IF EXISTS stream_memory_test'));
    $conn->close();
});

describe('Stream Memory', function (): void {
    describe('Text Protocol', function (): void {

        it('streams 100k rows without significant memory growth', function (): void {
            $conn = makeConnection();
            $totalRows = 100000;
            $threshold = 5 * 1024 * 1024;

            gc_collect_cycles();
            $startMemory = memory_get_usage();

            $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

            $count = 0;
            $peakMemory = 0;

            foreach ($stream as $row) {
                $count++;

                if ($count === 1) {
                    expect(isset($row['uuid']))->toBeTrue();
                }

                if ($count % 10000 === 0) {
                    $peakMemory = max($peakMemory, memory_get_usage());
                }
            }

            $memoryGrowth = $peakMemory - $startMemory;

            expect($count)->toBe($totalRows)
                ->and($memoryGrowth)->toBeLessThan($threshold)
            ;

            $conn->close();
        });

        it('stream stats report correct row count and a valid duration', function (): void {
            $conn = makeConnection();
            $totalRows = 100000;

            $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

            foreach ($stream as $_) {
                // consume
            }

            $stats = $stream->getStats();

            expect($stats->rowCount)->toBe($totalRows)
                ->and($stats->duration)->toBeGreaterThan(0.0)
            ;

            $conn->close();
        });

        it('streams rows with correct column structure', function (): void {
            $conn = makeConnection();
            $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test LIMIT 10'));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(10);

            foreach ($rows as $row) {
                expect($row)->toHaveKey('id')
                    ->and($row)->toHaveKey('uuid')
                    ->and($row)->toHaveKey('description')
                    ->and($row)->toHaveKey('created_at')
                    ->and($row['description'])->toBe(str_repeat('X', 100))
                ;
            }

            $conn->close();
        });

        it('peak PHP memory stays under 20MB while streaming 100k rows', function (): void {
            $conn = makeConnection();
            $threshold = 20 * 1024 * 1024;

            gc_collect_cycles();
            $baselineMemory = memory_get_usage();

            $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

            $peakMemory = 0;
            foreach ($stream as $_) {
                $peakMemory = max($peakMemory, memory_get_usage());
            }

            $memoryGrowth = $peakMemory - $baselineMemory;

            expect($memoryGrowth)->toBeLessThan($threshold);

            $conn->close();
        });
    });

    describe('Binary Protocol', function (): void {

        it('streams 100k rows via prepared statement without significant memory growth', function (): void {
            $conn = makeConnection();
            $totalRows = 100000;
            $threshold = 5 * 1024 * 1024;

            gc_collect_cycles();
            $startMemory = memory_get_usage();

            $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
            $stream = await($stmt->executeStream([]));

            $count = 0;
            $peakMemory = 0;

            foreach ($stream as $row) {
                $count++;

                if ($count === 1) {
                    expect(isset($row['uuid']))->toBeTrue();
                }

                if ($count % 10000 === 0) {
                    $peakMemory = max($peakMemory, memory_get_usage());
                }
            }

            $memoryGrowth = $peakMemory - $startMemory;

            expect($count)->toBe($totalRows)
                ->and($memoryGrowth)->toBeLessThan($threshold)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('binary stream stats report correct row count and a valid duration', function (): void {
            $conn = makeConnection();
            $totalRows = 100000;

            $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
            $stream = await($stmt->executeStream([]));

            foreach ($stream as $_) {
                // consume
            }

            $stats = $stream->getStats();

            expect($stats->rowCount)->toBe($totalRows)
                ->and($stats->duration)->toBeGreaterThan(0.0)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('binary stream rows have correct column structure', function (): void {
            $conn = makeConnection();

            $stmt = await($conn->prepare('SELECT * FROM stream_memory_test LIMIT 10'));
            $stream = await($stmt->executeStream([]));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(10);

            foreach ($rows as $row) {
                expect($row)->toHaveKey('id')
                    ->and($row)->toHaveKey('uuid')
                    ->and($row)->toHaveKey('description')
                    ->and($row)->toHaveKey('created_at')
                ;
            }

            await($stmt->close());
            $conn->close();
        });

        it('binary stream memory stays stable with small buffer size', function (): void {
            $conn = makeConnection();
            $threshold = 5 * 1024 * 1024;

            gc_collect_cycles();
            $startMemory = memory_get_usage();

            $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
            $stream = await($stmt->executeStream([], bufferSize: 10));

            $peakMemory = 0;
            foreach ($stream as $_) {
                $peakMemory = max($peakMemory, memory_get_usage());
            }

            $memoryGrowth = $peakMemory - $startMemory;

            expect($memoryGrowth)->toBeLessThan($threshold);

            await($stmt->close());
            $conn->close();
        });
    });

    describe('Stream Stats', function (): void {

        describe('Text Protocol', function (): void {

            it('stats have correct row count and column count', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();

                expect($stats->rowCount)->toBe(100000)
                    ->and($stats->columnCount)->toBe(4) // id, uuid, description, created_at
                ;

                $conn->close();
            });

            it('stats have a positive duration after streaming', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();

                expect($stats->duration)->toBeGreaterThan(0.0);

                $conn->close();
            });

            it('stats warningCount defaults to zero', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test LIMIT 10'));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();

                expect($stats->warningCount)->toBe(0);

                $conn->close();
            });

            it('hasRows returns true when rows were streamed', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test LIMIT 10'));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->hasRows())->toBeTrue();

                $conn->close();
            });

            it('hasRows returns false on empty result set', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery(
                    'SELECT * FROM stream_memory_test WHERE id = -1'
                ));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->hasRows())->toBeFalse();

                $conn->close();
            });

            it('getRowsPerSecond returns a positive value after streaming', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();

                expect($stats->getRowsPerSecond())->toBeGreaterThan(0.0);

                $conn->close();
            });

            it('getRowsPerSecond returns zero when row count is zero', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery(
                    'SELECT * FROM stream_memory_test WHERE id = -1'
                ));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->getRowsPerSecond())->toBe(0.0);

                $conn->close();
            });

            it('rows per second is consistent with row count and duration', function (): void {
                $conn = makeConnection();
                $stream = await($conn->streamQuery('SELECT * FROM stream_memory_test'));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();
                $expected = $stats->rowCount / $stats->duration;

                expect($stats->getRowsPerSecond())->toBe($expected);

                $conn->close();
            });
        });

        describe('Binary Protocol', function (): void {

            it('binary stats have correct row count and column count', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();

                expect($stats->rowCount)->toBe(100000)
                    ->and($stats->columnCount)->toBe(4)
                ;

                await($stmt->close());
                $conn->close();
            });

            it('binary stats have a positive duration after streaming', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->duration)->toBeGreaterThan(0.0);

                await($stmt->close());
                $conn->close();
            });

            it('binary stats warningCount defaults to zero', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare('SELECT * FROM stream_memory_test LIMIT 10'));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->warningCount)->toBe(0);

                await($stmt->close());
                $conn->close();
            });

            it('binary hasRows returns true when rows were streamed', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare('SELECT * FROM stream_memory_test LIMIT 10'));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->hasRows())->toBeTrue();

                await($stmt->close());
                $conn->close();
            });

            it('binary hasRows returns false on empty result set', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare(
                    'SELECT * FROM stream_memory_test WHERE id = -1'
                ));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->hasRows())->toBeFalse();

                await($stmt->close());
                $conn->close();
            });

            it('binary getRowsPerSecond returns a positive value after streaming', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->getRowsPerSecond())->toBeGreaterThan(0.0);

                await($stmt->close());
                $conn->close();
            });

            it('binary getRowsPerSecond returns zero when row count is zero', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare(
                    'SELECT * FROM stream_memory_test WHERE id = -1'
                ));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                expect($stream->getStats()->getRowsPerSecond())->toBe(0.0);

                await($stmt->close());
                $conn->close();
            });

            it('binary rows per second is consistent with row count and duration', function (): void {
                $conn = makeConnection();
                $stmt = await($conn->prepare('SELECT * FROM stream_memory_test'));
                $stream = await($stmt->executeStream([]));

                foreach ($stream as $_) {
                    // consume
                }

                $stats = $stream->getStats();
                $expected = $stats->rowCount / $stats->duration;

                expect($stats->getRowsPerSecond())->toBe($expected);

                await($stmt->close());
                $conn->close();
            });
        });
    });
});
