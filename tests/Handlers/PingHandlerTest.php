<?php

declare(strict_types=1);

namespace Hibla\Mysql\Tests\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Handlers\PingHandler;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Mockery;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

describe('PingHandler', function () {
    it('creates ping handler successfully', function () {
        $socket = Mockery::mock(SocketConnection::class);

        $handler = new PingHandler($socket);

        expect($handler)->toBeInstanceOf(PingHandler::class);
    });

    it('starts ping and writes packet to socket', function () {
        $socket = Mockery::mock(SocketConnection::class);

        $socket->shouldReceive('write')->once()->andReturnUsing(function ($packet) {
            expect(strlen($packet))->toBe(5);
            expect(ord($packet[4]))->toBe(0x0E);
            return true;
        });

        $handler = new PingHandler($socket);
        $promise = new Promise();

        $handler->start($promise);

        expect(true)->toBeTrue();
    });

    it('resolves promise with true on OK packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new PingHandler($socket);
        $promise = new Promise();

        $handler->start($promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x00);
        $payloadReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(0, 0, 0);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0);
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('');

        $handler->processPacket($payloadReader, 7, 1);

        $result = null;
        $promise->then(function ($r) use (&$result) {
            $result = $r;
        });

        Loop::run();

        expect($result)->toBeTrue();
    });

    it('rejects promise on ERR packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new PingHandler($socket);
        $promise = new Promise();

        $handler->start($promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFF);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(2006);
        $payloadReader->shouldReceive('readFixedString')->with(1)->andReturn('#');
        $payloadReader->shouldReceive('readFixedString')->with(5)->andReturn('08S01');
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('Server shutdown in progress');

        $handler->processPacket($payloadReader, 40, 1);

        $errorMessage = '';
        $promise->catch(function ($e) use (&$errorMessage) {
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($errorMessage)->toContain('Ping failed')->and($errorMessage)->toContain('Server shutdown in progress');
    });

    it('rejects promise on unexpected packet type', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new PingHandler($socket);
        $promise = new Promise();

        $handler->start($promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $payloadReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('');

        $handler->processPacket($payloadReader, 5, 1);

        $errorMessage = '';
        $promise->catch(function ($e) use (&$errorMessage) {
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($errorMessage)->toContain('Unexpected packet type');
    });

    it('rejects promise when processPacket throws', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $handler = new PingHandler($socket);
        $promise = new Promise();

        $handler->start($promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andThrow(new \Exception('Malformed packet'));

        $handler->processPacket($payloadReader, 1, 1);

        $errorMessage = '';
        $promise->catch(function ($e) use (&$errorMessage) {
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($errorMessage)->toContain('Malformed packet');
    });
});