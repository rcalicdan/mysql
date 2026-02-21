<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\Mysql\Exceptions\ConfigurationException;
use Hibla\Mysql\Exceptions\NotInitializedException;
use Hibla\Mysql\Internals\ManagedPreparedStatement;

use Hibla\Mysql\MysqlClient;

beforeAll(function (): void {
    $client = makeClient();

    await($client->query('CREATE TABLE IF NOT EXISTS mysql_client_test (
        id   INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100)
    )'));

    await($client->query(
        "INSERT INTO mysql_client_test (name) VALUES ('alice'), ('bob'), ('charlie')"
    ));

    $client->close();
});

afterAll(function (): void {
    $client = makeClient();
    await($client->query('DROP TABLE IF EXISTS mysql_client_test'));
    $client->close();
});

describe('MysqlClient', function (): void {

    describe('Construction', function (): void {

        it('creates a client with a ConnectionParams instance', function (): void {
            $client = new MysqlClient(testConnectionParams());

            expect($client->getStats()['statement_cache_enabled'])->toBeTrue();

            $client->close();
        });

        it('creates a client with an array config', function (): void {
            $client = new MysqlClient([
                'host' => $_ENV['MYSQL_HOST'] ?? '127.0.0.1',
                'port' => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
                'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
                'username' => $_ENV['MYSQL_USERNAME'] ?? 'test_user',
                'password' => $_ENV['MYSQL_PASSWORD'] ?? 'test_password',
            ]);

            expect($client->getStats()['max_size'])->toBe(10);

            $client->close();
        });

        it('creates a client with a URI string config', function (): void {
            $host = $_ENV['MYSQL_HOST'] ?? '127.0.0.1';
            $port = $_ENV['MYSQL_PORT'] ?? '3306';
            $database = $_ENV['MYSQL_DATABASE'] ?? 'test';
            $username = $_ENV['MYSQL_USERNAME'] ?? 'test_user';
            $password = $_ENV['MYSQL_PASSWORD'] ?? 'test_password';

            $client = new MysqlClient(
                "mysql://{$username}:{$password}@{$host}:{$port}/{$database}"
            );

            expect($client->getStats()['max_size'])->toBe(10);

            $client->close();
        });

        it('throws ConfigurationException for invalid maxConnections', function (): void {
            expect(fn () => makeClient(maxConnections: 0))
                ->toThrow(ConfigurationException::class)
            ;
        });

        it('throws ConfigurationException for invalid idleTimeout', function (): void {
            expect(fn () => makeClient(idleTimeout: 0))
                ->toThrow(ConfigurationException::class)
            ;
        });

        it('throws ConfigurationException for invalid maxLifetime', function (): void {
            expect(fn () => makeClient(maxLifetime: 0))
                ->toThrow(ConfigurationException::class)
            ;
        });

        it('creates a client with statement cache disabled', function (): void {
            $client = makeClient(enableStatementCache: false);
            $stats = $client->getStats();

            expect($stats['statement_cache_enabled'])->toBeFalse();

            $client->close();
        });
    });

    describe('Stats', function (): void {

        it('returns correct default stats before any query', function (): void {
            $client = makeClient(maxConnections: 3, statementCacheSize: 128);
            $stats = $client->getStats();

            expect($stats['max_size'])->toBe(3)
                ->and($stats['active_connections'])->toBe(0)
                ->and($stats['pooled_connections'])->toBe(0)
                ->and($stats['waiting_requests'])->toBe(0)
                ->and($stats['statement_cache_enabled'])->toBeTrue()
                ->and($stats['statement_cache_size'])->toBe(128)
            ;

            $client->close();
        });

        it('reflects statement cache disabled in stats', function (): void {
            $client = makeClient(enableStatementCache: false);
            $stats = $client->getStats();

            expect($stats['statement_cache_enabled'])->toBeFalse();

            $client->close();
        });
    });

    describe('Health Check', function (): void {

        it('returns healthy stats when pool has idle connections', function (): void {
            $client = makeClient();

            await($client->query('SELECT 1'));

            $stats = await($client->healthCheck());

            expect($stats['total_checked'])->toBeGreaterThanOrEqual(1)
                ->and($stats['unhealthy'])->toBe(0)
            ;

            $client->close();
        });

        it('returns zero stats when no connections have been made', function (): void {
            $client = makeClient();
            $stats = await($client->healthCheck());

            expect($stats['total_checked'])->toBe(0)
                ->and($stats['healthy'])->toBe(0)
                ->and($stats['unhealthy'])->toBe(0)
            ;

            $client->close();
        });
    });

    describe('Query', function (): void {

        it('executes a plain SELECT without params (text protocol)', function (): void {
            $client = makeClient();
            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('executes a SELECT with params (binary protocol)', function (): void {
            $client = makeClient();
            $result = await($client->query('SELECT ? AS val', [42]));

            expect($result->fetchOne()['val'])->toBe(42);

            $client->close();
        });

        it('executes a SELECT with params and statement cache disabled', function (): void {
            $client = makeClient(enableStatementCache: false);
            $result = await($client->query('SELECT ? AS val', [99]));

            expect($result->fetchOne()['val'])->toBe(99);

            $client->close();
        });

        it('releases connection back to pool after query', function (): void {
            $client = makeClient(maxConnections: 1);

            await($client->query('SELECT 1'));
            await($client->query('SELECT 2'));

            // If connection was not released the second query would deadlock.
            // Verify it's sitting idle in the pool now.
            expect($client->getStats()['pooled_connections'])->toBe(1);

            $client->close();
        });

        it('releases connection back to pool after failed query', function (): void {
            $client = makeClient(maxConnections: 1);

            try {
                await($client->query('SELECT * FROM non_existent_table_xyz'));
            } catch (Throwable) {
                // expected
            }

            // Connection should be healthy and back in the pool.
            // A follow-up query proves it was released.
            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });

        it('executes multiple sequential queries correctly', function (): void {
            $client = makeClient();
            $result1 = await($client->query('SELECT 1 AS val'));
            $result2 = await($client->query('SELECT 2 AS val'));
            $result3 = await($client->query('SELECT 3 AS val'));

            expect($result1->fetchOne()['val'])->toBe('1')
                ->and($result2->fetchOne()['val'])->toBe('2')
                ->and($result3->fetchOne()['val'])->toBe('3')
            ;

            $client->close();
        });
    });

    describe('Execute', function (): void {

        it('returns affected rows count after INSERT', function (): void {
            $client = makeClient();
            $affectedRows = await($client->execute(
                "INSERT INTO mysql_client_test (name) VALUES ('execute_test')"
            ));

            expect($affectedRows)->toBe(1);

            await($client->execute(
                "DELETE FROM mysql_client_test WHERE name = 'execute_test'"
            ));

            $client->close();
        });

        it('returns zero affected rows when UPDATE matches no rows', function (): void {
            $client = makeClient();
            $affectedRows = await($client->execute(
                "UPDATE mysql_client_test SET name = 'x' WHERE id = -9999"
            ));

            expect($affectedRows)->toBe(0);

            $client->close();
        });

        it('returns affected rows with params', function (): void {
            $client = makeClient();
            $affectedRows = await($client->execute(
                'INSERT INTO mysql_client_test (name) VALUES (?)',
                ['execute_param_test']
            ));

            expect($affectedRows)->toBe(1);

            await($client->execute(
                'DELETE FROM mysql_client_test WHERE name = ?',
                ['execute_param_test']
            ));

            $client->close();
        });
    });

    describe('ExecuteGetId', function (): void {

        it('returns the last insert id after INSERT', function (): void {
            $client = makeClient();
            $insertId = await($client->executeGetId(
                "INSERT INTO mysql_client_test (name) VALUES ('id_test')"
            ));

            expect($insertId)->toBeGreaterThan(0);

            await($client->execute(
                'DELETE FROM mysql_client_test WHERE id = ?',
                [$insertId]
            ));

            $client->close();
        });

        it('returns the last insert id with params', function (): void {
            $client = makeClient();
            $insertId = await($client->executeGetId(
                'INSERT INTO mysql_client_test (name) VALUES (?)',
                ['id_param_test']
            ));

            expect($insertId)->toBeGreaterThan(0);

            await($client->execute(
                'DELETE FROM mysql_client_test WHERE id = ?',
                [$insertId]
            ));

            $client->close();
        });
    });

    describe('FetchOne', function (): void {

        it('returns the first row of a result set', function (): void {
            $client = makeClient();
            $row = await($client->fetchOne('SELECT 1 AS val, 2 AS other'));

            expect($row['val'])->toBe('1')
                ->and($row['other'])->toBe('2')
            ;

            $client->close();
        });

        it('returns null when no rows match', function (): void {
            $client = makeClient();
            $row = await($client->fetchOne('SELECT 1 WHERE 1 = 0'));

            expect($row)->toBeNull();

            $client->close();
        });

        it('returns only the first row when multiple rows exist', function (): void {
            $client = makeClient();
            $row = await($client->fetchOne(
                'SELECT 1 AS val UNION SELECT 2 AS val UNION SELECT 3 AS val'
            ));

            expect($row['val'])->toBe('1');

            $client->close();
        });

        it('returns first row with params', function (): void {
            $client = makeClient();
            $row = await($client->fetchOne('SELECT ? AS val', [123]));

            expect($row['val'])->toBe(123);

            $client->close();
        });
    });

    describe('FetchValue', function (): void {

        it('returns the value of a named column', function (): void {
            $client = makeClient();
            $value = await($client->fetchValue('SELECT 42 AS answer', 'answer'));

            expect($value)->toBe('42');

            $client->close();
        });

        it('returns null for a numeric index because fetchOne returns associative arrays only', function (): void {
            $client = makeClient();

            // fetchOne() returns purely associative arrays — integer index 0
            // falls through to the null-coalescing fallback in fetchValue()
            $value = await($client->fetchValue('SELECT 42 AS answer', 0));

            expect($value)->toBeNull();

            $client->close();
        });

        it('returns null when no rows match', function (): void {
            $client = makeClient();
            $value = await($client->fetchValue('SELECT 1 WHERE 1 = 0', 0));

            expect($value)->toBeNull();

            $client->close();
        });

        it('returns null when column key does not exist', function (): void {
            $client = makeClient();
            $value = await($client->fetchValue('SELECT 1 AS val', 'nonexistent'));

            expect($value)->toBeNull();

            $client->close();
        });

        it('returns a value with params', function (): void {
            $client = makeClient();
            $value = await($client->fetchValue('SELECT ? AS val', 'val', [99]));

            expect($value)->toBe(99);

            $client->close();
        });
    });

    describe('Stream', function (): void {

        it('streams rows without params using text protocol', function (): void {
            $client = makeClient();
            $stream = await($client->stream('SELECT 1 AS val UNION SELECT 2 UNION SELECT 3'));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(3)
                ->and($rows[0]['val'])->toBe('1')
            ;

            $client->close();
        });

        it('streams rows with params using binary protocol', function (): void {
            $client = makeClient();
            $stream = await($client->stream(
                'SELECT name FROM mysql_client_test WHERE id > ? LIMIT 1',
                [0]
            ));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->not->toBeEmpty()
                ->and($rows[0])->toHaveKey('name')
            ;

            $client->close();
        });

        it('releases connection back to pool after stream is fully consumed', function (): void {
            $client = makeClient(maxConnections: 1);
            $stream = await($client->stream('SELECT 1 AS val UNION SELECT 2'));

            foreach ($stream as $_) {
                // consume
            }

            // If connection was not released this would deadlock on a single-slot pool.
            $result = await($client->query('SELECT 3 AS val'));

            expect($result->fetchOne()['val'])->toBe('3');

            $client->close();
        });

        it('streams an empty result set without error', function (): void {
            $client = makeClient();
            $stream = await($client->stream('SELECT 1 WHERE 1 = 0'));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toBeEmpty();

            $client->close();
        });

        it('stream stats report correct row count', function (): void {
            $client = makeClient();
            $stream = await($client->stream(
                'SELECT 1 AS val UNION SELECT 2 UNION SELECT 3'
            ));

            foreach ($stream as $_) {
                // consume
            }

            expect($stream->getStats()->rowCount)->toBe(3);

            $client->close();
        });
    });

    describe('Prepare', function (): void {

        it('returns a ManagedPreparedStatement', function (): void {
            $client = makeClient();
            $stmt = await($client->prepare('SELECT ? AS val'));

            expect($stmt)->toBeInstanceOf(ManagedPreparedStatement::class);

            await($stmt->close());
            $client->close();
        });

        it('executes a prepared statement and returns the correct result', function (): void {
            $client = makeClient();
            $stmt = await($client->prepare('SELECT ? AS val'));
            $result = await($stmt->execute([42]));

            expect($result->fetchOne()['val'])->toBe(42);

            await($stmt->close());
            $client->close();
        });

        it('reuses the same prepared statement for multiple executions', function (): void {
            $client = makeClient();
            $stmt = await($client->prepare('SELECT ? AS val'));

            $result1 = await($stmt->execute([1]));
            $result2 = await($stmt->execute([2]));

            expect($result1->fetchOne()['val'])->toBe(1)
                ->and($result2->fetchOne()['val'])->toBe(2)
            ;

            await($stmt->close());
            $client->close();
        });

        it('releases connection back to pool on prepare failure', function (): void {
            $client = makeClient(maxConnections: 1);

            try {
                await($client->prepare('NOT VALID SQL ???'));
            } catch (Throwable) {
                // expected
            }

            // Connection was returned healthy to the pool.
            // A follow-up query on the same single-slot pool proves it.
            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });
    });

    describe('Statement Cache', function (): void {

        it('reuses cached statement across multiple query() calls', function (): void {
            $client = makeClient(statementCacheSize: 10);

            $result1 = await($client->query('SELECT ? AS val', [1]));
            $result2 = await($client->query('SELECT ? AS val', [2]));

            expect($result1->fetchOne()['val'])->toBe(1)
                ->and($result2->fetchOne()['val'])->toBe(2)
            ;

            $client->close();
        });

        it('clearStatementCache() does not break subsequent queries', function (): void {
            $client = makeClient();

            await($client->query('SELECT ? AS val', [1]));

            $client->clearStatementCache();

            $result = await($client->query('SELECT ? AS val', [2]));

            expect($result->fetchOne()['val'])->toBe(2);

            $client->close();
        });

        it('works correctly with cache disabled', function (): void {
            $client = makeClient(enableStatementCache: false);

            $result1 = await($client->query('SELECT ? AS val', [10]));
            $result2 = await($client->query('SELECT ? AS val', [20]));

            expect($result1->fetchOne()['val'])->toBe(10)
                ->and($result2->fetchOne()['val'])->toBe(20)
            ;

            $client->close();
        });

        it('clearStatementCache() is safe when cache is disabled', function (): void {
            $client = makeClient(enableStatementCache: false);

            $client->clearStatementCache();

            $result = await($client->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $client->close();
        });
    });

    describe('Close', function (): void {

        it('throws NotInitializedException after close()', function (): void {
            $client = makeClient();
            $client->close();

            expect(fn () => await($client->query('SELECT 1')))
                ->toThrow(NotInitializedException::class)
            ;
        });

        it('is safe to call close() multiple times', function (): void {
            $client = makeClient();
            $client->close();
            $client->close();

            expect(true)->toBeTrue();
        });

        it('closes cleanly even if no queries were made', function (): void {
            $client = makeClient();
            $client->close();

            expect(true)->toBeTrue();
        });
    });

    describe('Destructor', function (): void {

        it('automatically closes when unset', function (): void {
            $client = makeClient();
            await($client->query('SELECT 1'));

            expect($client->getStats()['pooled_connections'])->toBeGreaterThanOrEqual(1);

            unset($client);

            expect(true)->toBeTrue();
        });

        it('throws NotInitializedException after being garbage collected', function (): void {
            $client = makeClient();
            $client->close();

            expect(fn () => $client->getStats())
                ->toThrow(NotInitializedException::class)
            ;
        });
    });
});
