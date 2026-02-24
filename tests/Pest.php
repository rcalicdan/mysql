<?php

declare(strict_types=1);

use function Hibla\await;

use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Manager\PoolManager;
use Hibla\Mysql\MysqlClient;

use Hibla\Mysql\ValueObjects\ConnectionParams;

uses()
    ->afterEach(function () {
        Mockery::close();
    })
    ->in(__DIR__)
;

function createConnectionParams(bool $ssl = false): ConnectionParams
{
    return new ConnectionParams(
        host: 'localhost',
        port: 3306,
        username: 'testuser',
        password: 'testpass',
        database: 'testdb',
        ssl: $ssl
    );
}

function buildMySQLHandshakeV10Packet(bool $supportsSSL = false): string
{
    $payload = '';
    $payload .= chr(10);
    $payload .= "8.0.32\0";
    $payload .= pack('V', 123);
    $payload .= '12345678';
    $payload .= "\0";

    $capabilities = 0xF7DF;
    if ($supportsSSL) {
        $capabilities |= 0x0800;
    }
    $payload .= pack('v', $capabilities);
    $payload .= chr(255);
    $payload .= pack('v', 2);
    $payload .= pack('v', 0x8000);
    $payload .= chr(21);
    $payload .= str_repeat("\0", 10);
    $payload .= "123456789012\0";
    $payload .= "caching_sha2_password\0";

    $length = strlen($payload);
    $header = substr(pack('V', $length), 0, 3) . chr(0);

    return $header . $payload;
}

function buildMySQLOkPacket(): string
{
    $payload = chr(0x00) . chr(0x00) . chr(0x00) . pack('v', 0x0002) . pack('v', 0x0000);
    $length = strlen($payload);
    $header = substr(pack('V', $length), 0, 3) . chr(2);

    return $header . $payload;
}

function buildMySQLResultSetHeaderPacket(int $columnCount): string
{
    $payload = chr($columnCount);
    $length = strlen($payload);
    $header = substr(pack('V', $length), 0, 3) . chr(1);

    return $header . $payload;
}

function buildMySQLErrPacket(int $errorCode, string $errorMessage): string
{
    $payload = chr(0xFF);
    $payload .= pack('v', $errorCode);
    $payload .= '#';
    $payload .= '28000';
    $payload .= $errorMessage;

    $length = strlen($payload);
    $header = substr(pack('V', $length), 0, 3) . chr(1);

    return $header . $payload;
}

function testConnectionParams(): ConnectionParams
{
    return ConnectionParams::fromArray([
        'host' => $_ENV['MYSQL_HOST'] ?? '127.0.0.1',
        'port' => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
        'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
        'username' => $_ENV['MYSQL_USERNAME'] ?? 'test_user',
        'password' => $_ENV['MYSQL_PASSWORD'] ?? 'test_password',
    ]);
}

function makeConnection(): Connection
{
    $conn = await(Connection::create(testConnectionParams()));

    return $conn;
}

function makePool(int $maxSize = 5, int $idleTimeout = 300, int $maxLifetime = 3600): PoolManager
{
    return new PoolManager(testConnectionParams(), $maxSize, $idleTimeout, $maxLifetime);
}

function makeClient(
    int $maxConnections = 5,
    int $idleTimeout = 300,
    int $maxLifetime = 3600,
    int $statementCacheSize = 256,
    bool $enableStatementCache = true
): MysqlClient {
    return new MysqlClient(
        testConnectionParams(),
        $maxConnections,
        $idleTimeout,
        $maxLifetime,
        $statementCacheSize,
        $enableStatementCache
    );
}

function makeTransactionClient(int $maxConnections = 5): MysqlClient
{
    return new MysqlClient(
        testConnectionParams(),
        $maxConnections,
        300,
        3600
    );
}

function makeConcurrentClient(int $maxConnections = 10): MysqlClient
{
    return new MysqlClient(
        testConnectionParams(),
        $maxConnections,
        300,
        3600
    );
}

function makeCompressedClient(
    int $maxConnections = 5,
    int $idleTimeout = 300,
    int $maxLifetime = 3600,
    int $statementCacheSize = 256,
    bool $enableStatementCache = true
): MysqlClient {
    $params = ConnectionParams::fromArray([
        'host' => $_ENV['MYSQL_HOST'] ?? '127.0.0.1',
        'port' => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
        'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
        'username' => $_ENV['MYSQL_USERNAME'] ?? 'test_user',
        'password' => $_ENV['MYSQL_PASSWORD'] ?? 'test_password',
        'compress' => true,
    ]);

    return new MysqlClient(
        $params,
        $maxConnections,
        $idleTimeout,
        $maxLifetime,
        $statementCacheSize,
        $enableStatementCache
    );
}

function twentyRowSql(): string
{
    return '
            SELECT n
            FROM (
                SELECT (a.N + b.N * 10 + 1) AS n
                FROM
                    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                     UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
                    (SELECT 0 AS N UNION SELECT 1) b
            ) numbers
            ORDER BY n
        ';
}

function twentyRowPreparedSql(): string
{
    return '
            SELECT n
            FROM (
                SELECT (a.N + b.N * 10 + 1) AS n
                FROM
                    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                     UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
                    (SELECT 0 AS N UNION SELECT 1) b
            ) numbers
            ORDER BY n
        ';
}
