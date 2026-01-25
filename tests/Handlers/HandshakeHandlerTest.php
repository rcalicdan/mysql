<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\MysqlClient\Handlers\HandshakeHandler;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;
use Tests\Fixtures\SslCapableConnection;

describe('HandshakeHandler', function () {
    it('creates handshake handler successfully', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $params = createConnectionParams();

        $handler = new HandshakeHandler($socket, $params);

        expect($handler)->toBeInstanceOf(HandshakeHandler::class);
    });

    it('start returns a promise', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $params = createConnectionParams();
        $handler = new HandshakeHandler($socket, $params);
        
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();

        $result = $handler->start($packetReader);

        expect($result)->toBeInstanceOf(PromiseInterface::class);
    });

    it('rejects when socket does not support encryption during SSL upgrade', function () {
        $params = createConnectionParams(ssl: true);

        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new HandshakeHandler($socket, $params);
        
        $handshakePacket = buildMySQLHandshakeV10Packet(supportsSSL: true);
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $promise = $handler->start($packetReader);

        $rejected = false;
        $errorMessage = '';

        $promise->catch(function ($e) use (&$rejected, &$errorMessage) {
            $rejected = true;
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($rejected)->toBeTrue()
            ->and($errorMessage)->toContain('SSL/TLS upgrade');
    });

    it('writes SSL request packet when SSL is enabled', function () {
        $params = createConnectionParams(ssl: true);

        $socket = Mockery::mock(SslCapableConnection::class);

        $socket->shouldReceive('write')->twice()->andReturnUsing(function ($packet) {
            expect(strlen($packet))->toBeGreaterThan(0);
            return true;
        });

        $socket->shouldReceive('enableEncryption')
            ->once()
            ->andReturn(Promise::resolved());

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet(supportsSSL: true);
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $handler->start($packetReader);

        Loop::run();
    });

    it('sends auth response without SSL when SSL is disabled', function () {
        $params = createConnectionParams(ssl: false);

        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet(supportsSSL: false);
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $handler->start($packetReader);

        expect(true)->toBeTrue();
    });

    it('resolves promise on successful authentication with OK packet', function () {
        $params = createConnectionParams();
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet();
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $promise = $handler->start($packetReader);

        $okPacket = buildMySQLOkPacket();
        $packetReader->append($okPacket);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x00);

        $handler->processPacket($payloadReader, 7, 2);

        $resolved = false;
        $promise->then(function ($sequenceId) use (&$resolved) {
            $resolved = true;
            expect($sequenceId)->toBe(3);
        });

        Loop::run();

        expect($resolved)->toBeTrue();
    });

    it('rejects promise on authentication error with ERR packet', function () {
        $params = createConnectionParams();
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet();
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $promise = $handler->start($packetReader);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFF); // ERR
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(1045);
        $payloadReader->shouldReceive('readFixedString')->with(1)->andReturn('#');
        $payloadReader->shouldReceive('readFixedString')->with(5)->andReturn('28000');
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('Access denied');

        $handler->processPacket($payloadReader, 50, 2);

        $rejected = false;
        $errorMessage = '';

        $promise->catch(function ($e) use (&$rejected, &$errorMessage) {
            $rejected = true;
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($rejected)->toBeTrue()
            ->and($errorMessage)->toContain('MySQL Handshake Error')
            ->and($errorMessage)->toContain('1045');
    });

    it('handles auth switch request', function () {
        $params = createConnectionParams();
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->twice();

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet();
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $handler->start($packetReader);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE); // SWITCH
        $payloadReader->shouldReceive('readNullTerminatedString')->andReturn('mysql_native_password');
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('scramble');

        $handler->processPacket($payloadReader, 30, 2);

        expect(true)->toBeTrue();
    });

    it('handles fast auth success status', function () {
        $params = createConnectionParams();
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet();
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $handler->start($packetReader);

        $payloadReader = Mockery::mock(PayloadReader::class);
    
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01, 0x03);

        $handler->processPacket($payloadReader, 2, 2);

        expect(true)->toBeTrue();
    });

    it('handles full auth required over SSL', function () {
        $params = createConnectionParams(ssl: true);
        
        $socket = Mockery::mock(SslCapableConnection::class);

        $socket->shouldReceive('write')->times(3);
        $socket->shouldReceive('enableEncryption')->once()->andReturn(Promise::resolved());

        $handler = new HandshakeHandler($socket, $params);

        $handshakePacket = buildMySQLHandshakeV10Packet(supportsSSL: true);
        $packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $packetReader->append($handshakePacket);

        $handler->start($packetReader);

        Loop::run();

        $payloadReader = Mockery::mock(PayloadReader::class);
    
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01, 0x04);

        $handler->processPacket($payloadReader, 2, 2);

        expect(true)->toBeTrue();
    });
});