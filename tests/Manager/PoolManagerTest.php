<?php

declare(strict_types=1);

use Hibla\Mysql\Exceptions\PoolException;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Socket\Connector;

use function Hibla\await;
use function Hibla\sleep;

describe('PoolManager', function (): void {
    describe('Construction and Validation', function (): void {

        it('creates a pool with default settings', function (): void {
            $pool = makePool();
            $stats = $pool->getStats();

            expect($stats['max_size'])->toBe(5)
                ->and($stats['active_connections'])->toBe(0)
                ->and($stats['pooled_connections'])->toBe(0)
                ->and($stats['waiting_requests'])->toBe(0)
                ->and($stats['config_validated'])->toBeTrue();

            $pool->close();
        });

        it('accepts a ConnectionParams instance directly', function (): void {
            $pool  = new PoolManager(testConnectionParams(), 3);
            $stats = $pool->getStats();

            expect($stats['max_size'])->toBe(3);

            $pool->close();
        });

        it('accepts an array config', function (): void {
            $pool = new PoolManager([
                'host'     => $_ENV['MYSQL_HOST']     ?? '127.0.0.1',
                'port'     => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
                'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
                'username' => $_ENV['MYSQL_USERNAME'] ?? 'test_user',
                'password' => $_ENV['MYSQL_PASSWORD'] ?? 'test_password',
            ], 3);

            expect($pool->getStats()['max_size'])->toBe(3);

            $pool->close();
        });

        it('accepts a URI string config', function (): void {
            $host     = $_ENV['MYSQL_HOST']     ?? '127.0.0.1';
            $port     = $_ENV['MYSQL_PORT']     ?? '3306';
            $database = $_ENV['MYSQL_DATABASE'] ?? 'test';
            $username = $_ENV['MYSQL_USERNAME'] ?? 'test_user';
            $password = $_ENV['MYSQL_PASSWORD'] ?? 'test_password';

            $pool = new PoolManager(
                "mysql://{$username}:{$password}@{$host}:{$port}/{$database}",
                3
            );

            expect($pool->getStats()['max_size'])->toBe(3);

            $pool->close();
        });

        it('throws InvalidArgumentException when maxSize is zero', function (): void {
            expect(fn() => makePool(0))
                ->toThrow(\InvalidArgumentException::class, 'Pool max size must be greater than 0');
        });

        it('throws InvalidArgumentException when maxSize is negative', function (): void {
            expect(fn() => makePool(-1))
                ->toThrow(\InvalidArgumentException::class, 'Pool max size must be greater than 0');
        });

        it('throws InvalidArgumentException when idleTimeout is zero', function (): void {
            expect(fn() => new PoolManager(testConnectionParams(), 5, 0))
                ->toThrow(\InvalidArgumentException::class, 'Idle timeout must be greater than 0');
        });

        it('throws InvalidArgumentException when idleTimeout is negative', function (): void {
            expect(fn() => new PoolManager(testConnectionParams(), 5, -1))
                ->toThrow(\InvalidArgumentException::class, 'Idle timeout must be greater than 0');
        });

        it('throws InvalidArgumentException when maxLifetime is zero', function (): void {
            expect(fn() => new PoolManager(testConnectionParams(), 5, 300, 0))
                ->toThrow(\InvalidArgumentException::class, 'Max lifetime must be greater than 0');
        });

        it('throws InvalidArgumentException when maxLifetime is negative', function (): void {
            expect(fn() => new PoolManager(testConnectionParams(), 5, 300, -1))
                ->toThrow(\InvalidArgumentException::class, 'Max lifetime must be greater than 0');
        });

        it('accepts a custom connector instance', function (): void {
            $connector = new Connector([
                'tcp'            => true,
                'tls'            => false,
                'unix'           => false,
                'dns'            => true,
                'happy_eyeballs' => false,
            ]);

            $pool = new PoolManager(testConnectionParams(), 3, 300, 3600, $connector);
            $conn = await($pool->get());

            expect($conn)->toBeInstanceOf(Connection::class)
                ->and($conn->isReady())->toBeTrue();

            $pool->release($conn);
            $pool->close();
        });
    });

    describe('Acquiring Connections', function (): void {

        it('gets a connection from the pool', function (): void {
            $pool = makePool();
            $conn = await($pool->get());

            expect($conn)->toBeInstanceOf(Connection::class)
                ->and($conn->isReady())->toBeTrue();

            $pool->release($conn);
            $pool->close();
        });

        it('increments active connections on get', function (): void {
            $pool = makePool();
            $conn = await($pool->get());

            expect($pool->getStats()['active_connections'])->toBe(1);

            $pool->release($conn);
            $pool->close();
        });

        it('creates multiple connections up to maxSize', function (): void {
            $pool  = makePool(3);
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());
            $conn3 = await($pool->get());

            expect($pool->getStats()['active_connections'])->toBe(3);

            $pool->release($conn1);
            $pool->release($conn2);
            $pool->release($conn3);
            $pool->close();
        });

        it('reuses a released connection instead of creating a new one', function (): void {
            $pool  = makePool(3);
            $conn1 = await($pool->get());
            $pool->release($conn1);

            $stats1 = $pool->getStats();

            $conn2 = await($pool->get());

            $stats2 = $pool->getStats();

            expect($stats1['pooled_connections'])->toBe(1)
                ->and($stats2['active_connections'])->toBe(1);

            $pool->release($conn2);
            $pool->close();
        });

        it('queues requests when pool is at capacity', function (): void {
            $pool  = makePool(2);
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());

            $pending = $pool->get();

            expect($pool->getStats()['waiting_requests'])->toBe(1);

            $pool->release($conn1);

            $conn3 = await($pending);

            expect($conn3)->toBeInstanceOf(Connection::class)
                ->and($pool->getStats()['waiting_requests'])->toBe(0);

            $pool->release($conn2);
            $pool->release($conn3);
            $pool->close();
        });

        it('resolves waiters in FIFO order', function (): void {
            $pool  = makePool(1);
            $conn1 = await($pool->get());

            $resolved = [];

            $pending1 = $pool->get()->then(function (Connection $c) use (&$resolved, $pool): void {
                $resolved[] = 1;
                $pool->release($c);
            });

            $pending2 = $pool->get()->then(function (Connection $c) use (&$resolved, $pool): void {
                $resolved[] = 2;
                $pool->release($c);
            });

            $pool->release($conn1);

            await($pending1);
            await($pending2);

            expect($resolved)->toBe([1, 2]);

            $pool->close();
        });
    });

    describe('Releasing Connections', function (): void {

        it('returns a connection to the pool on release', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            expect($pool->getStats()['pooled_connections'])->toBe(1);

            $pool->close();
        });

        it('pauses the connection on release when no waiters exist', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            expect($conn->isClosed())->toBeFalse();

            $pool->close();
        });

        it('removes a closed connection on release', function (): void {
            $pool = makePool();
            $conn = await($pool->get());

            $conn->close();
            $pool->release($conn);

            expect($pool->getStats()['pooled_connections'])->toBe(0)
                ->and($pool->getStats()['active_connections'])->toBe(0);

            $pool->close();
        });

        it('passes connection directly to waiter on release', function (): void {
            $pool  = makePool(1);
            $conn1 = await($pool->get());

            $pending = $pool->get();

            expect($pool->getStats()['waiting_requests'])->toBe(1);

            $pool->release($conn1);

            $conn2 = await($pending);

            expect($conn2)->toBeInstanceOf(Connection::class)
                ->and($pool->getStats()['waiting_requests'])->toBe(0)
                ->and($pool->getStats()['pooled_connections'])->toBe(0);

            $pool->release($conn2);
            $pool->close();
        });

        it('decrements active connections when releasing a closed connection', function (): void {
            $pool = makePool();
            $conn = await($pool->get());

            expect($pool->getStats()['active_connections'])->toBe(1);

            $conn->close();
            $pool->release($conn);

            expect($pool->getStats()['active_connections'])->toBe(0);

            $pool->close();
        });
    });

    describe('Statistics', function (): void {

        it('returns correct stats with no connections', function (): void {
            $pool  = makePool(10);
            $stats = $pool->getStats();

            expect($stats['active_connections'])->toBe(0)
                ->and($stats['pooled_connections'])->toBe(0)
                ->and($stats['waiting_requests'])->toBe(0)
                ->and($stats['max_size'])->toBe(10)
                ->and($stats['config_validated'])->toBeTrue()
                ->and($stats['tracked_connections'])->toBe(0);

            $pool->close();
        });

        it('tracks active connections correctly across get and release', function (): void {
            $pool  = makePool(5);
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());

            expect($pool->getStats()['active_connections'])->toBe(2);

            $pool->release($conn1);

            expect($pool->getStats()['active_connections'])->toBe(2)
                ->and($pool->getStats()['pooled_connections'])->toBe(1);

            $pool->release($conn2);

            expect($pool->getStats()['pooled_connections'])->toBe(2);

            $pool->close();
        });

        it('tracks connection creation timestamps', function (): void {
            $pool  = makePool();
            $conn  = await($pool->get());
            $stats = $pool->getStats();

            expect($stats['tracked_connections'])->toBe(1);

            $pool->release($conn);
            $pool->close();
        });
    });

    describe('Health Check', function (): void {

        it('returns healthy stats when pool has active connections', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            expect($pool->getStats()['pooled_connections'])->toBe(1);

            $stats = await($pool->healthCheck());

            expect($stats['total_checked'])->toBe(1)
                ->and($stats['healthy'])->toBeGreaterThanOrEqual(0)
                ->and($stats['unhealthy'])->toBe(0)
                ->and($stats['total_checked'])->toBe($stats['healthy'] + $stats['unhealthy']);

            $pool->close();
        });

        it('returns zero stats when pool is empty', function (): void {
            $pool  = makePool();
            $stats = await($pool->healthCheck());

            expect($stats['total_checked'])->toBe(0)
                ->and($stats['healthy'])->toBe(0)
                ->and($stats['unhealthy'])->toBe(0);

            $pool->close();
        });

        it('returns healthy stats for multiple pooled connections', function (): void {
            $pool  = makePool(3);
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());
            $conn3 = await($pool->get());

            $pool->release($conn1);
            $pool->release($conn2);
            $pool->release($conn3);

            expect($pool->getStats()['pooled_connections'])->toBe(3);

            $stats = await($pool->healthCheck());

            expect($stats['total_checked'])->toBe(3)
                ->and($stats['healthy'] + $stats['unhealthy'])->toBe($stats['total_checked'])
                ->and($stats['unhealthy'])->toBe(0);

            $pool->close();
        });

        it('keeps healthy connections in pool after health check', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            await($pool->healthCheck());

            expect($pool->getStats()['pooled_connections'])->toBe(1);

            $pool->close();
        });

        it('connections are usable after health check', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            await($pool->healthCheck());

            $conn2   = await($pool->get());
            $result  = await($conn2->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $pool->release($conn2);
            $pool->close();
        });
    });

    describe('Close', function (): void {

        it('closes all pooled connections on close()', function (): void {
            $pool  = makePool();
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());

            $pool->release($conn1);
            $pool->release($conn2);

            $pool->close();

            expect($conn1->isClosed())->toBeTrue()
                ->and($conn2->isClosed())->toBeTrue();
        });

        it('resets stats to zero after close()', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            $pool->close();

            $stats = $pool->getStats();

            expect($stats['active_connections'])->toBe(0)
                ->and($stats['pooled_connections'])->toBe(0)
                ->and($stats['waiting_requests'])->toBe(0)
                ->and($stats['tracked_connections'])->toBe(0);
        });

        it('rejects pending waiters with PoolException on close()', function (): void {
            $pool  = makePool(1);
            $conn  = await($pool->get());
            $error = null;

            $pending = $pool->get();
            $pending->then(null, function (\Throwable $e) use (&$error): void {
                $error = $e;
            });

            expect($pool->getStats()['waiting_requests'])->toBe(1);

            $pool->release($conn);
            $resolvedConn = await($pending);
            $pool->release($resolvedConn);
            $pool->close();

            expect($error)->toBeNull();

            $pool2  = makePool(1);
            $conn2  = await($pool2->get());
            $error2 = null;

            $pending2 = $pool2->get();

            $chained = $pending2->then(null, function (\Throwable $e) use (&$error2): void {
                $error2 = $e;
            });

            expect($pool2->getStats()['waiting_requests'])->toBe(1);

            $pool2->close();

            try {
                await($chained);
            } catch (\Throwable) {
                // expected rejection
            }

            expect($error2)->toBeInstanceOf(PoolException::class);
        });

        it('is safe to call close() multiple times', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            $pool->close();
            $pool->close();

            expect(true)->toBeTrue();
        });
    });

    describe('Concurrent Usage', function (): void {

        it('handles multiple sequential get and release cycles', function (): void {
            $pool = makePool(3);

            for ($i = 0; $i < 10; $i++) {
                $conn   = await($pool->get());
                $result = await($conn->query('SELECT 1 AS val'));
                expect($result->fetchOne()['val'])->toBe('1');
                $pool->release($conn);
            }

            expect($pool->getStats()['active_connections'])->toBe(1)
                ->and($pool->getStats()['pooled_connections'])->toBe(1);

            $pool->close();
        });

        it('does not exceed maxSize under concurrent requests', function (): void {
            $pool        = makePool(3);
            $connections = [];

            for ($i = 0; $i < 3; $i++) {
                $connections[] = await($pool->get());
            }

            expect($pool->getStats()['active_connections'])->toBe(3);

            // A 4th request should be queued not create a new connection
            $pending = $pool->get();
            expect($pool->getStats()['waiting_requests'])->toBe(1)
                ->and($pool->getStats()['active_connections'])->toBe(3);

            $pool->release($connections[0]);

            $conn4 = await($pending);
            expect($pool->getStats()['active_connections'])->toBe(3)
                ->and($pool->getStats()['waiting_requests'])->toBe(0);

            $pool->release($conn4);
            $pool->release($connections[1]);
            $pool->release($connections[2]);
            $pool->close();
        });

        it('can execute queries across multiple pooled connections', function (): void {
            $pool  = makePool(3);
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());
            $conn3 = await($pool->get());

            $result1 = await($conn1->query('SELECT 1 AS val'));
            $result2 = await($conn2->query('SELECT 2 AS val'));
            $result3 = await($conn3->query('SELECT 3 AS val'));

            expect($result1->fetchOne()['val'])->toBe('1')
                ->and($result2->fetchOne()['val'])->toBe('2')
                ->and($result3->fetchOne()['val'])->toBe('3');

            $pool->release($conn1);
            $pool->release($conn2);
            $pool->release($conn3);
            $pool->close();
        });

        it('pool recovers after connections are closed externally', function (): void {
            $pool = makePool(2);
            $conn = await($pool->get());

            $conn->close();
            $pool->release($conn);

            expect($pool->getStats()['active_connections'])->toBe(0);

            $newConn = await($pool->get());
            expect($newConn->isReady())->toBeTrue();

            $pool->release($newConn);
            $pool->close();
        });
    });

    describe('Idle Timeout and Max Lifetime', function (): void {

        it('discards a connection that exceeds idle timeout on next get', function (): void {
            $pool = new PoolManager(testConnectionParams(), 5, 1, 3600);
            $conn = await($pool->get());
            $pool->release($conn);

            expect($pool->getStats()['pooled_connections'])->toBe(1);

            sleep(1.1);

            $newConn = await($pool->get());

            expect($newConn->isReady())->toBeTrue();

            $pool->release($newConn);
            $pool->close();
        });

        it('discards a connection that exceeds max lifetime on release', function (): void {
            $pool = new PoolManager(testConnectionParams(), 5, 300, 1);
            $conn = await($pool->get());

            sleep(1.1);

            $pool->release($conn);

            expect($pool->getStats()['pooled_connections'])->toBe(0)
                ->and($pool->getStats()['active_connections'])->toBe(0);

            $pool->close();
        });

        it('discards expired connection on get and creates a fresh one', function (): void {
            $pool = new PoolManager(testConnectionParams(), 5, 300, 1);
            $conn = await($pool->get());
            $pool->release($conn);

            sleep(1.1);

            $newConn = await($pool->get());

            expect($newConn->isReady())->toBeTrue()
                ->and($newConn->isClosed())->toBeFalse();

            $pool->release($newConn);
            $pool->close();
        });
    });

    describe('Destructor', function (): void {

        it('automatically closes pooled connections when pool is unset', function (): void {
            $pool = makePool();
            $conn = await($pool->get());
            $pool->release($conn);

            expect($pool->getStats()['pooled_connections'])->toBe(1);

            unset($pool);

            expect($conn->isClosed())->toBeTrue();
        });

        it('automatically closes multiple pooled connections when pool is unset', function (): void {
            $pool  = makePool(3);
            $conn1 = await($pool->get());
            $conn2 = await($pool->get());
            $conn3 = await($pool->get());

            $pool->release($conn1);
            $pool->release($conn2);
            $pool->release($conn3);

            expect($pool->getStats()['pooled_connections'])->toBe(3);

            unset($pool);

            expect($conn1->isClosed())->toBeTrue()
                ->and($conn2->isClosed())->toBeTrue()
                ->and($conn3->isClosed())->toBeTrue();
        });

        it('rejects pending waiters when pool is garbage collected', function (): void {
            $pool  = makePool(1);
            $conn  = await($pool->get());
            $error = null;

            $pending = $pool->get();
            $chained = $pending->then(null, function (\Throwable $e) use (&$error): void {
                $error = $e;
            });

            expect($pool->getStats()['waiting_requests'])->toBe(1);

            unset($pool);

            try {
                await($chained);
            } catch (\Throwable) {
                // expected
            }

            expect($error)->toBeInstanceOf(PoolException::class);

            $conn->close();
        });
    });
});
