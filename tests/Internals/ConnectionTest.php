<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Internals\PreparedStatement;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\Internals\RowStream;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Socket\Connector;

use Hibla\Sql\Exceptions\ConnectionException;

beforeAll(function (): void {
    $conn = makeConnection();

    await($conn->query('DROP TABLE IF EXISTS pest_users'));
    await($conn->query('
        CREATE TABLE pest_users (
            id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name       VARCHAR(100) NOT NULL,
            email      VARCHAR(150) NOT NULL UNIQUE,
            age        TINYINT UNSIGNED NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    '));

    $conn->close();
});

afterAll(function (): void {
    $conn = makeConnection();
    await($conn->query('DROP TABLE IF EXISTS pest_users'));
    $conn->close();
});

describe('Connection', function (): void {

    beforeEach(function (): void {
        $conn = makeConnection();
        await($conn->query('TRUNCATE TABLE pest_users'));
        $conn->close();
    });

    describe('Pause and Resume', function (): void {

        it('can pause and resume a connection without error', function (): void {
            $conn = makeConnection();

            $conn->pause();
            $conn->resume();

            expect($conn->isReady())->toBeTrue()
                ->and($conn->isClosed())->toBeFalse()
            ;

            $conn->close();
        });

        it('can execute a query after resuming a paused connection', function (): void {
            $conn = makeConnection();

            $conn->pause();
            $conn->resume();

            $result = await($conn->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $conn->close();
        });

        it('can pause and resume multiple times and still query', function (): void {
            $conn = makeConnection();

            $conn->pause();
            $conn->resume();
            $conn->pause();
            $conn->resume();
            $conn->pause();
            $conn->resume();

            $result = await($conn->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $conn->close();
        });

        it('can execute a prepared statement after resuming', function (): void {
            $conn = makeConnection();

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('PauseUser', 'pause@example.com', 45)"
            ));

            $conn->pause();
            $conn->resume();

            $stmt = await($conn->prepare(
                'SELECT name, age FROM pest_users WHERE email = ?'
            ));
            $result = await($stmt->execute(['pause@example.com']));
            $row = $result->fetchOne();

            expect($row['name'])->toBe('PauseUser')
                ->and((int) $row['age'])->toBe(45)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('can stream a query after resuming a paused connection', function (): void {
            $conn = makeConnection();

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('ResumeStream', 'resumestream@example.com', 60)"
            ));

            $conn->pause();
            $conn->resume();

            $stream = await($conn->streamQuery('SELECT name FROM pest_users ORDER BY id'));

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(1)
                ->and($rows[0]['name'])->toBe('ResumeStream')
            ;

            $conn->close();
        });

        it('pausing does not lose connection state', function (): void {
            $conn = makeConnection();

            expect($conn->isReady())->toBeTrue();

            $conn->pause();

            expect($conn->isReady())->toBeTrue()
                ->and($conn->isClosed())->toBeFalse()
            ;

            $conn->resume();

            $conn->close();
        });
    });

    describe('Connectivity', function (): void {

        it('connects to MySQL and returns a ready Connection', function (): void {
            $conn = makeConnection();

            expect($conn)->toBeInstanceOf(Connection::class)
                ->and($conn->isReady())->toBeTrue()
                ->and($conn->isClosed())->toBeFalse()
            ;

            $conn->close();
        });

        it('marks the connection as closed after close()', function (): void {
            $conn = makeConnection();
            $conn->close();

            expect($conn->isClosed())->toBeTrue()
                ->and($conn->isReady())->toBeFalse()
            ;
        });

        it('can ping the server', function (): void {
            $conn = makeConnection();

            expect(await($conn->ping()))->toBeTrue();

            $conn->close();
        });
    });

    describe('Plain Query', function (): void {

        it('executes SELECT 1', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 AS val'));

            expect($result)->toBeInstanceOf(Result::class)
                ->and($result->fetchOne()['val'])->toBe('1')
            ;

            $conn->close();
        });

        it('confirms the test table exists', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SHOW TABLES LIKE 'pest_users'"));

            expect($result->rowCount())->toBe(1);

            $conn->close();
        });

        it('inserts a row and returns affected rows and last insert id', function (): void {
            $conn = makeConnection();
            $result = await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)"
            ));

            expect($result->getAffectedRows())->toBe(1)
                ->and($result->getLastInsertId())->toBeGreaterThan(0)
            ;

            $conn->close();
        });

        it('selects inserted rows', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)"
            ));

            $result = await($conn->query('SELECT * FROM pest_users'));
            $row = $result->fetchOne();

            expect($result->rowCount())->toBe(1)
                ->and($row['name'])->toBe('Bob')
                ->and($row['email'])->toBe('bob@example.com')
                ->and((int) $row['age'])->toBe(25)
            ;

            $conn->close();
        });

        it('updates a row', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 20)"
            ));

            $update = await($conn->query(
                "UPDATE pest_users SET age = 21 WHERE email = 'charlie@example.com'"
            ));
            expect($update->getAffectedRows())->toBe(1);

            $select = await($conn->query('SELECT age FROM pest_users LIMIT 1'));
            expect((int) $select->fetchOne()['age'])->toBe(21);

            $conn->close();
        });

        it('deletes a row', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Dave', 'dave@example.com', 40)"
            ));

            $delete = await($conn->query(
                "DELETE FROM pest_users WHERE email = 'dave@example.com'"
            ));
            expect($delete->getAffectedRows())->toBe(1);

            $count = await($conn->query('SELECT COUNT(*) AS cnt FROM pest_users'));
            expect((int) $count->fetchOne()['cnt'])->toBe(0);

            $conn->close();
        });
    });

    describe('Parameterized Query', function (): void {

        it('prepares a statement and returns the correct param count', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));

            expect($stmt)->toBeInstanceOf(PreparedStatement::class)
                ->and($stmt->numParams)->toBe(3)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('executes a prepared INSERT', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));
            $result = await($stmt->execute(['Eve', 'eve@example.com', 28]));

            expect($result->getAffectedRows())->toBe(1)
                ->and($result->getLastInsertId())->toBeGreaterThan(0)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('executes a prepared SELECT', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Frank', 'frank@example.com', 35)"
            ));

            $stmt = await($conn->prepare(
                'SELECT name, email, age FROM pest_users WHERE email = ?'
            ));
            $result = await($stmt->execute(['frank@example.com']));
            $row = $result->fetchOne();

            expect($result->rowCount())->toBe(1)
                ->and($row['name'])->toBe('Frank')
                ->and((int) $row['age'])->toBe(35)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('reuses the same prepared statement multiple times', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));

            $users = [
                ['Grace', 'grace@example.com', 22],
                ['Heidi', 'heidi@example.com', 31],
                ['Ivan',  'ivan@example.com',  19],
            ];

            foreach ($users as $user) {
                $result = await($stmt->execute($user));
                expect($result->getAffectedRows())->toBe(1);
            }

            await($stmt->close());

            $count = await($conn->query('SELECT COUNT(*) AS cnt FROM pest_users'));
            expect((int) $count->fetchOne()['cnt'])->toBe(3);

            $conn->close();
        });

        it('throws InvalidArgumentException when param count does not match', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));

            expect(fn () => $stmt->execute(['Judy', 'judy@example.com']))
                ->toThrow(InvalidArgumentException::class)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('executes a prepared UPDATE', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Karl', 'karl@example.com', 50)"
            ));

            $stmt = await($conn->prepare('UPDATE pest_users SET age = ? WHERE email = ?'));
            $result = await($stmt->execute([51, 'karl@example.com']));
            expect($result->getAffectedRows())->toBe(1);

            await($stmt->close());

            $select = await($conn->query('SELECT age FROM pest_users LIMIT 1'));
            expect((int) $select->fetchOne()['age'])->toBe(51);

            $conn->close();
        });

        it('executes a prepared DELETE', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Laura', 'laura@example.com', 29)"
            ));

            $stmt = await($conn->prepare('DELETE FROM pest_users WHERE email = ?'));
            $result = await($stmt->execute(['laura@example.com']));
            expect($result->getAffectedRows())->toBe(1);

            await($stmt->close());

            $count = await($conn->query('SELECT COUNT(*) AS cnt FROM pest_users'));
            expect((int) $count->fetchOne()['cnt'])->toBe(0);

            $conn->close();
        });
    });

    describe('Stream Query', function (): void {

        function seedUsers(Connection $conn, int $count): void
        {
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));

            for ($i = 1; $i <= $count; $i++) {
                await($stmt->execute(["User{$i}", "user{$i}@example.com", $i]));
            }

            await($stmt->close());
        }

        it('streams a plain SELECT row by row', function (): void {
            $conn = makeConnection();
            seedUsers($conn, 5);

            $stream = await($conn->streamQuery(
                'SELECT name, age FROM pest_users ORDER BY id'
            ));

            expect($stream)->toBeInstanceOf(RowStream::class);

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(5)
                ->and($rows[0]['name'])->toBe('User1')
                ->and($rows[4]['name'])->toBe('User5')
            ;

            $conn->close();
        });

        it('stream yields correct column values in order', function (): void {
            $conn = makeConnection();
            seedUsers($conn, 3);

            $stream = await($conn->streamQuery(
                'SELECT age FROM pest_users ORDER BY age ASC'
            ));

            $ages = [];
            foreach ($stream as $row) {
                $ages[] = (int) $row['age'];
            }

            expect($ages)->toBe([1, 2, 3]);

            $conn->close();
        });

        it('streams an empty result set without error', function (): void {
            $conn = makeConnection();
            $stream = await($conn->streamQuery('SELECT * FROM pest_users'));

            $count = 0;
            foreach ($stream as $_) {
                $count++;
            }

            expect($count)->toBe(0);

            $conn->close();
        });

        it('streams a parameterized query with executeStream()', function (): void {
            $conn = makeConnection();
            seedUsers($conn, 5);

            $stmt = await($conn->prepare(
                'SELECT name, age FROM pest_users WHERE age > ? ORDER BY age'
            ));
            $stream = await($stmt->executeStream([2]));

            expect($stream)->toBeInstanceOf(RowStream::class);

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(3)
                ->and($rows[0]['name'])->toBe('User3')
                ->and($rows[2]['name'])->toBe('User5')
            ;

            await($stmt->close());
            $conn->close();
        });

        it('stream exposes stats after full iteration', function (): void {
            $conn = makeConnection();
            seedUsers($conn, 3);

            $stream = await($conn->streamQuery('SELECT * FROM pest_users'));

            foreach ($stream as $_) {
                // consume all rows
            }

            expect($stream->getStats())->not->toBeNull();

            $conn->close();
        });

        it('stream respects buffer size without deadlocking', function (): void {
            $conn = makeConnection();
            seedUsers($conn, 10);

            $stream = await($conn->streamQuery(
                'SELECT name FROM pest_users ORDER BY id',
                bufferSize: 2
            ));

            $names = [];
            foreach ($stream as $row) {
                $names[] = $row['name'];
            }

            expect($names)->toHaveCount(10)
                ->and($names[0])->toBe('User1')
                ->and($names[9])->toBe('User10')
            ;

            $conn->close();
        });
    });

    describe('Custom Connector', function (): void {

        function makeCustomConnector(): Connector
        {
            return new Connector([
                'tcp' => true,
                'tls' => false,
                'unix' => false,
                'dns' => true,
                'happy_eyeballs' => false,
            ]);
        }

        it('connects using a custom ConnectorInterface instance', function (): void {
            $conn = await(Connection::create(testConnectionParams(), makeCustomConnector()));

            expect($conn)->toBeInstanceOf(Connection::class)
                ->and($conn->isReady())->toBeTrue()
                ->and($conn->isClosed())->toBeFalse()
            ;

            $conn->close();
        });

        it('can ping using a custom connector', function (): void {
            $conn = await(Connection::create(testConnectionParams(), makeCustomConnector()));

            expect(await($conn->ping()))->toBeTrue();

            $conn->close();
        });

        it('can execute a plain query using a custom connector', function (): void {
            $conn = await(Connection::create(testConnectionParams(), makeCustomConnector()));
            $result = await($conn->query('SELECT 1 AS val'));

            expect($result->fetchOne()['val'])->toBe('1');

            $conn->close();
        });

        it('can INSERT and SELECT using a custom connector', function (): void {
            $conn = await(Connection::create(testConnectionParams(), makeCustomConnector()));

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('CustomConn', 'custom@example.com', 99)"
            ));

            $result = await($conn->query(
                "SELECT * FROM pest_users WHERE email = 'custom@example.com'"
            ));

            $row = $result->fetchOne();

            expect($result->rowCount())->toBe(1)
                ->and($row['name'])->toBe('CustomConn')
                ->and((int) $row['age'])->toBe(99)
            ;

            $conn->close();
        });

        it('can use a prepared statement using a custom connector', function (): void {
            $conn = await(Connection::create(testConnectionParams(), makeCustomConnector()));

            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));
            $result = await($stmt->execute(['ConnUser', 'connuser@example.com', 55]));

            expect($result->getAffectedRows())->toBe(1)
                ->and($result->getLastInsertId())->toBeGreaterThan(0)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('can stream a query using a custom connector', function (): void {
            $conn = await(Connection::create(testConnectionParams(), makeCustomConnector()));

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('StreamConn', 'streamconn@example.com', 77)"
            ));

            $stream = await($conn->streamQuery('SELECT name FROM pest_users ORDER BY id'));

            expect($stream)->toBeInstanceOf(RowStream::class);

            $rows = [];
            foreach ($stream as $row) {
                $rows[] = $row;
            }

            expect($rows)->toHaveCount(1)
                ->and($rows[0]['name'])->toBe('StreamConn')
            ;

            $conn->close();
        });

        it('rejects connection with wrong credentials using a custom connector', function (): void {
            $badParams = ConnectionParams::fromArray([
                'host' => $_ENV['MYSQL_HOST'] ?? '127.0.0.1',
                'port' => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
                'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
                'username' => 'wrong_user',
                'password' => 'wrong_password',
            ]);

            expect(fn () => await(Connection::create($badParams, makeCustomConnector())))
                ->toThrow(ConnectionException::class)
            ;
        });
    });

    describe('Raw Transaction', function (): void {

        it('commits a transaction and persists the data', function (): void {
            $conn = makeConnection();

            await($conn->query('BEGIN'));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Tom', 'tom@example.com', 30)"
            ));
            await($conn->query('COMMIT'));

            $verify = makeConnection();
            $result = await($verify->query("SELECT * FROM pest_users WHERE email = 'tom@example.com'"));

            expect($result->rowCount())->toBe(1)
                ->and($result->fetchOne()['name'])->toBe('Tom')
            ;

            $conn->close();
            $verify->close();
        });

        it('rolls back a transaction and discards the data', function (): void {
            $conn = makeConnection();

            await($conn->query('BEGIN'));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Jerry', 'jerry@example.com', 25)"
            ));
            await($conn->query('ROLLBACK'));

            $verify = makeConnection();
            $result = await($verify->query("SELECT * FROM pest_users WHERE email = 'jerry@example.com'"));

            expect($result->rowCount())->toBe(0);

            $conn->close();
            $verify->close();
        });

        it('commits multiple inserts in a single transaction', function (): void {
            $conn = makeConnection();

            await($conn->query('BEGIN'));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Anna', 'anna@example.com', 20)"
            ));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Beth', 'beth@example.com', 21)"
            ));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Carl', 'carl@example.com', 22)"
            ));
            await($conn->query('COMMIT'));

            $verify = makeConnection();
            $result = await($verify->query('SELECT COUNT(*) AS cnt FROM pest_users'));

            expect((int) $result->fetchOne()['cnt'])->toBe(3);

            $conn->close();
            $verify->close();
        });

        it('rolls back only the active transaction leaving prior commits intact', function (): void {
            $conn = makeConnection();

            await($conn->query('BEGIN'));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Dana', 'dana@example.com', 28)"
            ));
            await($conn->query('COMMIT'));

            await($conn->query('BEGIN'));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Evan', 'evan@example.com', 29)"
            ));
            await($conn->query('ROLLBACK'));

            $verify = makeConnection();
            $result = await($verify->query('SELECT name FROM pest_users ORDER BY name'));
            $names = array_column($result->fetchAll(), 'name');

            expect($names)->toBe(['Dana'])
                ->and($result->rowCount())->toBe(1)
            ;

            $conn->close();
            $verify->close();
        });

        it('can UPDATE inside a transaction and commit the change', function (): void {
            $conn = makeConnection();

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Felix', 'felix@example.com', 40)"
            ));

            await($conn->query('BEGIN'));
            await($conn->query(
                "UPDATE pest_users SET age = 41 WHERE email = 'felix@example.com'"
            ));
            await($conn->query('COMMIT'));

            $verify = makeConnection();
            $result = await($verify->query(
                "SELECT age FROM pest_users WHERE email = 'felix@example.com'"
            ));

            expect((int) $result->fetchOne()['age'])->toBe(41);

            $conn->close();
            $verify->close();
        });

        it('can UPDATE inside a transaction and roll back to original value', function (): void {
            $conn = makeConnection();

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Gina', 'gina@example.com', 50)"
            ));

            await($conn->query('BEGIN'));
            await($conn->query(
                "UPDATE pest_users SET age = 99 WHERE email = 'gina@example.com'"
            ));
            await($conn->query('ROLLBACK'));

            $verify = makeConnection();
            $result = await($verify->query(
                "SELECT age FROM pest_users WHERE email = 'gina@example.com'"
            ));

            expect((int) $result->fetchOne()['age'])->toBe(50);

            $conn->close();
            $verify->close();
        });

        it('can use savepoints to partially roll back within a transaction', function (): void {
            $conn = makeConnection();

            await($conn->query('BEGIN'));
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Hank', 'hank@example.com', 33)"
            ));

            await($conn->query('SAVEPOINT after_hank'));

            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Iris', 'iris@example.com', 34)"
            ));

            await($conn->query('ROLLBACK TO SAVEPOINT after_hank'));
            await($conn->query('COMMIT'));

            $verify = makeConnection();
            $result = await($verify->query('SELECT name FROM pest_users ORDER BY name'));
            $names = array_column($result->fetchAll(), 'name');

            expect($names)->toBe(['Hank'])
                ->and($result->rowCount())->toBe(1)
            ;

            $conn->close();
            $verify->close();
        });

        it('uses prepared statements inside a transaction', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));

            await($conn->query('BEGIN'));
            await($stmt->execute(['Jake', 'jake@example.com', 27]));
            await($stmt->execute(['Kate', 'kate@example.com', 26]));
            await($conn->query('COMMIT'));

            await($stmt->close());

            $verify = makeConnection();
            $result = await($verify->query('SELECT COUNT(*) AS cnt FROM pest_users'));

            expect((int) $result->fetchOne()['cnt'])->toBe(2);

            $conn->close();
            $verify->close();
        });

        it('rolls back prepared statement inserts within a transaction', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare(
                'INSERT INTO pest_users (name, email, age) VALUES (?, ?, ?)'
            ));

            await($conn->query('BEGIN'));
            await($stmt->execute(['Liam', 'liam@example.com', 18]));
            await($stmt->execute(['Mona', 'mona@example.com', 19]));
            await($conn->query('ROLLBACK'));

            await($stmt->close());

            $verify = makeConnection();
            $result = await($verify->query('SELECT COUNT(*) AS cnt FROM pest_users'));

            expect((int) $result->fetchOne()['cnt'])->toBe(0);

            $conn->close();
            $verify->close();
        });
    });

    describe('DDL / Table Management', function (): void {

        it('creates a new table and verifies it exists', function (): void {
            $conn = makeConnection();

            await($conn->query('DROP TABLE IF EXISTS pest_temp'));
            await($conn->query('
                CREATE TABLE pest_temp (
                    id   INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    data VARCHAR(255) NOT NULL
                )
            '));

            $result = await($conn->query("SHOW TABLES LIKE 'pest_temp'"));
            expect($result->rowCount())->toBe(1);

            await($conn->query('DROP TABLE IF EXISTS pest_temp'));
            $conn->close();
        });

        it('drops a table and confirms it is gone', function (): void {
            $conn = makeConnection();

            await($conn->query('CREATE TABLE IF NOT EXISTS pest_drop_me (id INT PRIMARY KEY)'));
            await($conn->query('DROP TABLE pest_drop_me'));

            $result = await($conn->query("SHOW TABLES LIKE 'pest_drop_me'"));
            expect($result->rowCount())->toBe(0);

            $conn->close();
        });

        it('alters a table to add a column', function (): void {
            $conn = makeConnection();

            await($conn->query('DROP TABLE IF EXISTS pest_alter'));
            await($conn->query('CREATE TABLE pest_alter (id INT PRIMARY KEY)'));
            await($conn->query('ALTER TABLE pest_alter ADD COLUMN label VARCHAR(50)'));

            $result = await($conn->query('DESCRIBE pest_alter'));
            $columns = array_column($result->fetchAll(), 'Field');

            expect($columns)->toContain('label');

            await($conn->query('DROP TABLE IF EXISTS pest_alter'));
            $conn->close();
        });

        it('truncates a table', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Temp', 'temp@example.com', 1)"
            ));
            await($conn->query('TRUNCATE TABLE pest_users'));

            $result = await($conn->query('SELECT COUNT(*) AS cnt FROM pest_users'));
            expect((int) $result->fetchOne()['cnt'])->toBe(0);

            $conn->close();
        });
    });

    describe('Result Object', function (): void {

        it('fetchAll returns every row', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES
                 ('Leo', 'leo@example.com', 10),
                 ('Mia', 'mia@example.com', 11)"
            ));

            $result = await($conn->query('SELECT name FROM pest_users ORDER BY name'));
            $all = $result->fetchAll();

            expect($all)->toHaveCount(2)
                ->and($all[0]['name'])->toBe('Leo')
                ->and($all[1]['name'])->toBe('Mia')
            ;

            $conn->close();
        });

        it('fetchAssoc advances the internal cursor', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES ('Nina', 'nina@example.com', 5)"
            ));

            $result = await($conn->query('SELECT name FROM pest_users'));
            $row1 = $result->fetchAssoc();
            $row2 = $result->fetchAssoc();

            expect($row1)->toBeArray()
                ->and($row1['name'])->toBe('Nina')
                ->and($row2)->toBeNull()
            ;

            $conn->close();
        });

        it('isEmpty returns true when no rows match', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT * FROM pest_users'));

            expect($result->isEmpty())->toBeTrue()
                ->and($result->rowCount())->toBe(0)
            ;

            $conn->close();
        });

        it('getColumnCount returns the number of selected columns', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 AS a, 2 AS b, 3 AS c'));

            expect($result->getColumnCount())->toBe(3);

            $conn->close();
        });

        it('fetchColumn returns a single column across all rows', function (): void {
            $conn = makeConnection();
            await($conn->query(
                "INSERT INTO pest_users (name, email, age) VALUES
                 ('Oscar', 'oscar@example.com', 7),
                 ('Paula', 'paula@example.com', 8)"
            ));

            $result = await($conn->query('SELECT name FROM pest_users ORDER BY name'));
            $names = $result->fetchColumn('name');

            expect($names)->toBe(['Oscar', 'Paula']);

            $conn->close();
        });

        it('getWarningCount is accessible', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1'));

            expect($result->getWarningCount())->toBeInt();

            $conn->close();
        });
    });

    describe('Large Packet Splitting', function (): void {

        it('round-trips a 20MB payload via prepared statement (packet split/reassembly)', function (): void {
            $conn = makeConnection();

            $serverLimit = await($conn->query("SHOW VARIABLES LIKE 'max_allowed_packet'"))
                ->fetchOne()['Value']
            ;

            $targetSize = 20 * 1024 * 1024;

            if ((int) $serverLimit < $targetSize) {
                $conn->close();
                $this->markTestSkipped(
                    'Server max_allowed_packet (' . number_format((int) $serverLimit / 1024 / 1024, 2) . 'MB) ' .
                        'is too small for this test. Increase it in my.cnf to at least 20MB.'
                );
            }

            $chunk = '0123456789ABCDEF';
            $data = str_repeat($chunk, (int) ($targetSize / strlen($chunk)));
            $data[0] = 'S';
            $data[$targetSize - 1] = 'E';

            $stmt = await($conn->prepare('SELECT ? as big_data'));
            $result = await($stmt->execute([$data]));
            $returned = $result->fetchOne()['big_data'];

            expect(strlen($returned))->toBe($targetSize)
                ->and($returned[0])->toBe('S')
                ->and($returned[$targetSize - 1])->toBe('E')
                ->and(md5($returned))->toBe(md5($data))
            ;

            await($stmt->close());
            $conn->close();
        });
    });

    describe('Connection Reset (COM_RESET_CONNECTION)', function (): void {

        it('reset() returns true', function (): void {
            $conn = makeResettableConnection();

            expect(await($conn->reset()))->toBeTrue();

            $conn->close();
        });

        it('clears session variables after reset', function (): void {
            $conn = makeResettableConnection();

            await($conn->query("SET @my_var = 'hello'"));

            $before = await($conn->query('SELECT @my_var AS val'));
            expect($before->fetchOne()['val'])->toBe('hello');

            await($conn->reset());

            $after = await($conn->query('SELECT @my_var AS val'));
            expect($after->fetchOne()['val'])->toBeNull();

            $conn->close();
        });

        it('clears multiple session variables after reset', function (): void {
            $conn = makeResettableConnection();

            await($conn->query('SET @a = 1, @b = 2, @c = 3'));

            await($conn->reset());

            $result = await($conn->query('SELECT @a AS a, @b AS b, @c AS c'));
            $row = $result->fetchOne();

            expect($row['a'])->toBeNull()
                ->and($row['b'])->toBeNull()
                ->and($row['c'])->toBeNull()
            ;

            $conn->close();
        });

        it('clears prepared statements after reset', function (): void {
            $conn = makeResettableConnection();

            $stmt = await($conn->prepare('SELECT ? AS val'));

            await($conn->reset());

            $error = null;

            try {
                await($stmt->execute([1]));
            } catch (Throwable $e) {
                $error = $e;
            } finally {
                try {
                    await($stmt->close());
                } catch (Throwable) {
                    //
                }
            }

            expect($error)->toBeInstanceOf(Throwable::class)
                ->and($error?->getMessage())->toContain('Unknown prepared statement handler')
            ;

            $conn->close();
        });

        it('connection is still usable after reset', function (): void {
            $conn = makeResettableConnection();

            await($conn->reset());

            $result = await($conn->query('SELECT 1 AS val'));
            expect($result->fetchOne()['val'])->toBe('1');

            $conn->close();
        });

        it('returns to READY state after reset', function (): void {
            $conn = makeResettableConnection();

            await($conn->reset());

            expect($conn->getState()->name)->toBe('READY');

            $conn->close();
        });

        it('clears session state but preserves the connection across multiple resets', function (): void {
            $conn = makeResettableConnection();

            for ($i = 1; $i <= 3; $i++) {
                await($conn->query("SET @val = {$i}"));
                $before = await($conn->query('SELECT @val AS v'));
                expect($before->fetchOne()['v'])->toBe((string) $i);

                await($conn->reset());

                $after = await($conn->query('SELECT @val AS v'));
                expect($after->fetchOne()['v'])->toBeNull();
            }

            expect($conn->getState()->name)->toBe('READY');

            $conn->close();
        });

        it('does not affect other connections when one resets', function (): void {
            $conn1 = makeResettableConnection();
            $conn2 = makeResettableConnection();

            await($conn1->query("SET @shared = 'conn1_value'"));
            await($conn2->query("SET @shared = 'conn2_value'"));

            await($conn1->reset());

            $r1 = await($conn1->query('SELECT @shared AS v'));
            expect($r1->fetchOne()['v'])->toBeNull();

            $r2 = await($conn2->query('SELECT @shared AS v'));
            expect($r2->fetchOne()['v'])->toBe('conn2_value');

            $conn1->close();
            $conn2->close();
        });
    });
});
