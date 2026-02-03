<?php

declare(strict_types=1);

namespace Hibla\Mysql\Tests\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Handlers\ExecuteHandler;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Mockery;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

describe('ExecuteHandler', function () {
    it('creates execute handler successfully', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $commandBuilder = new CommandBuilder();

        $handler = new ExecuteHandler($socket, $commandBuilder);

        expect($handler)->toBeInstanceOf(ExecuteHandler::class);
    });

    it('starts execution and writes packet to socket', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once()->andReturnUsing(function ($packet) {
            expect(strlen($packet))->toBeGreaterThan(0);
            return true;
        });

        $commandBuilder = new CommandBuilder();
        $handler = new ExecuteHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start(1, ['val1'], [], $promise);

        expect(true)->toBeTrue();
    });

    it('resolves promise with Result on OK packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new ExecuteHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start(1, [123], [], $promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x00);
        $payloadReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1, 456, 0);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0);
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('');

        $handler->processPacket($payloadReader, 7, 0);

        $result = null;
        $promise->then(function ($r) use (&$result) {
            $result = $r;
        });

        Loop::run();

        expect($result)->toBeInstanceOf(Result::class)
            ->and($result->getAffectedRows())->toBe(1)
            ->and($result->getLastInsertId())->toBe(456);
    });

    it('rejects promise on ERR packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new ExecuteHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start(1, [], [], $promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFF);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(1062);
        $payloadReader->shouldReceive('readFixedString')->with(1)->andReturn('#');
        $payloadReader->shouldReceive('readFixedString')->with(5)->andReturn('23000');
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('Duplicate entry');

        $handler->processPacket($payloadReader, 20, 0);

        $errorMessage = '';
        $promise->catch(function ($e) use (&$errorMessage) {
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($errorMessage)->toContain('Execute Error')->and($errorMessage)->toContain('Duplicate entry');
    });

    it('handles binary result set in buffered mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $colDef = new ColumnDefinition(
            catalog: 'def',
            schema: 'test_db',
            table: 'users',
            orgTable: 'users',
            name: 'id',
            orgName: 'id',
            charset: 63,
            columnLength: 11,
            type: 3, // LONG
            flags: 0,
            decimals: 0
        );

        $commandBuilder = new CommandBuilder();
        $handler = new ExecuteHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start(1, [], [$colDef], $promise);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $handler->processPacket($headerReader, 1, 0);

        $okReader = Mockery::mock(PayloadReader::class);
        $okReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x00);
        $okReader->shouldReceive('readFixedString')->with(1)->andReturn("\0"); 
        $okReader->shouldReceive('readFixedInteger')->with(4)->andReturn(100); 
        $handler->processPacket($okReader, 6, 1);

        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedString')->andReturn('');
        $handler->processPacket($eofReader, 5, 2);

        $result = null;
        $promise->then(function ($r) use (&$result) {
            $result = $r;
        });

        Loop::run();

        expect($result)->toBeInstanceOf(Result::class)
            ->and($result->count())->toBe(1)
            ->and($result->fetchOne()['id'])->toBe(100);
    });

    it('handles streaming mode with onRow callback', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $colDef = new ColumnDefinition(
            catalog: 'def',
            schema: 'test_db',
            table: 'users',
            orgTable: 'users',
            name: 'name',
            orgName: 'name',
            charset: 33,
            columnLength: 255,
            type: 253, 
            flags: 0,
            decimals: 0
        );

        $receivedRows = [];
        $streamContext = new StreamContext(
            onRow: function (array $row) use (&$receivedRows) {
                $receivedRows[] = $row;
            }
        );

        $commandBuilder = new CommandBuilder();
        $handler = new ExecuteHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start(1, [], [$colDef], $promise, $streamContext);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $handler->processPacket($headerReader, 1, 0);

        $rowReader = Mockery::mock(PayloadReader::class);
        $rowReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x00);
        $rowReader->shouldReceive('readFixedString')->with(1)->andReturn("\0");
        $rowReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn('Hibla');
        $handler->processPacket($rowReader, 10, 1);

        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedString')->andReturn('');
        $handler->processPacket($eofReader, 5, 2);

        $stats = null;
        $promise->then(function ($r) use (&$stats) {
            $stats = $r;
        });

        Loop::run();

        expect($stats)->toBeInstanceOf(StreamStats::class)
            ->and($stats->rowCount)->toBe(1)
            ->and($receivedRows[0]['name'])->toBe('Hibla');
    });

    it('triggers onError in streaming mode when parsing fails', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $errorTriggered = false;
        $streamContext = new StreamContext(
            onRow: function (array $row) {},
            onError: function () use (&$errorTriggered) {
                $errorTriggered = true;
            }
        );

        $commandBuilder = new CommandBuilder();
        $handler = new ExecuteHandler($socket, $commandBuilder);
        $promise = new Promise();

        $promise->catch(function () {});

        $handler->start(1, [], [], $promise, $streamContext);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andThrow(new \Exception('Malformed packet'));
        $headerReader->shouldReceive('readRestOfPacketString')->andReturn('');

        $handler->processPacket($headerReader, 1, 0);

        Loop::run();

        expect($errorTriggered)->toBeTrue();
    });
});