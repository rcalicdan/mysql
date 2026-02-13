<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\ValueObjects\ConnectionParams;

use function Hibla\await;

uses()
    ->afterEach(function () {
        Loop::stop();
        Loop::reset();
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
        'host'     => $_ENV['MYSQL_HOST']     ?? '127.0.0.1',
        'port'     => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
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