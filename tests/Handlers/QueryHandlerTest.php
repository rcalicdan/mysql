<?php

declare(strict_types=1);

namespace Hibla\Mysql\Tests\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Handlers\QueryHandler;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Mockery;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

describe('QueryHandler', function () {
    it('creates query handler successfully', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $commandBuilder = new CommandBuilder();

        $handler = new QueryHandler($socket, $commandBuilder);

        expect($handler)->toBeInstanceOf(QueryHandler::class);
    });

    it('executes query and writes packet to socket', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once()->andReturnUsing(function ($packet) {
            expect(strlen($packet))->toBeGreaterThan(0);

            return true;
        });

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT 1', $promise);

        expect(true)->toBeTrue();
    });

    it('resolves promise with Result on OK packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('INSERT INTO test (id) VALUES (1)', $promise);

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
            ->and($result->getLastInsertId())->toBe(456)
        ;
    });

    it('rejects promise on ERR packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT * FROM non_existent', $promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFF);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(1146);
        $payloadReader->shouldReceive('readFixedString')->with(1)->andReturn('#');
        $payloadReader->shouldReceive('readFixedString')->with(5)->andReturn('42S02');
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn("Table 'non_existent' doesn't exist");

        $handler->processPacket($payloadReader, 40, 0);

        $errorMessage = '';
        $promise->catch(function ($e) use (&$errorMessage) {
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($errorMessage)->toContain("Table 'non_existent' doesn't exist");
    });

    it('handles streaming mode with onRow callback', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $receivedRows = [];
        $streamContext = new StreamContext(
            onRow: function (array $row) use (&$receivedRows) {
                $receivedRows[] = $row;
            }
        );

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT name FROM users', $promise, $streamContext);

        // Header Packet (1 column)
        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $handler->processPacket($headerReader, 1, 0);

        // Column Definition Packet
        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(3, 253, 0);
        $colReader->shouldReceive('readFixedString')->with(3)->andReturn('def');
        $colReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn('db', 'table', 'orgTable', 'name', 'name');
        $colReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(12);
        $colReader->shouldReceive('readFixedInteger')->with(2)->andReturn(33, 0, 0);
        $colReader->shouldReceive('readFixedInteger')->with(4)->andReturn(255);
        $handler->processPacket($colReader, 20, 1);

        // INTERMEDIATE EOF PACKET (Ends Column Definitions)
        $intermediateEofReader = Mockery::mock(PayloadReader::class);
        $intermediateEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $intermediateEofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($intermediateEofReader, 5, 2);

        // Row Packet
        $rowReader = Mockery::mock(PayloadReader::class);
        $rowReader->shouldReceive('readFixedInteger')->with(1)->andReturn(5);
        $rowReader->shouldReceive('readFixedString')->with(5)->andReturn('Hibla');
        $handler->processPacket($rowReader, 6, 3);

        // Final EOF Packet
        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($eofReader, 5, 4);

        $resolved = false;
        $result = null;
        $promise->then(function ($r) use (&$resolved, &$result) {
            $resolved = true;
            $result = $r;
        });

        Loop::run();

        expect($resolved)->toBeTrue()
            ->and($result)->toBeInstanceOf(StreamStats::class)
            ->and($result->rowCount)->toBe(1)
            ->and($receivedRows)->toHaveCount(1)
            ->and($receivedRows[0]['name'])->toBe('Hibla')
        ;
    });

    it('calls onComplete callback in streaming mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $completeCalled = false;
        $streamContext = new StreamContext(
            onRow: function (array $row) {
            },
            onComplete: function (StreamStats $stats) use (&$completeCalled) {
                $completeCalled = true;
            }
        );

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT 1', $promise, $streamContext);

        // Header Packet
        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $handler->processPacket($headerReader, 1, 0);

        // Column Definition Packet
        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(3, 253, 0);
        $colReader->shouldReceive('readFixedString')->with(3)->andReturn('def');
        $colReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn('db', 'table', 'orgTable', 'name', 'name');
        $colReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(12);
        $colReader->shouldReceive('readFixedInteger')->with(2)->andReturn(33, 0, 0);
        $colReader->shouldReceive('readFixedInteger')->with(4)->andReturn(255);
        $handler->processPacket($colReader, 20, 1);

        // INTERMEDIATE EOF PACKET (Ends Column Definitions)
        $intermediateEofReader = Mockery::mock(PayloadReader::class);
        $intermediateEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $intermediateEofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($intermediateEofReader, 5, 2);

        // Final EOF Packet (Ends empty row set)
        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($eofReader, 5, 3);

        Loop::run();

        expect($completeCalled)->toBeTrue();
    });

    it('calls onError callback when exception occurs in streaming mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $errorCalled = false;
        $streamContext = new StreamContext(
            onRow: function (array $row) {
            },
            onError: function (\Throwable $e) use (&$errorCalled) {
                $errorCalled = true;
            }
        );

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();
        $promise->catch(function () {
        });

        $handler->start('SELECT 1', $promise, $streamContext);

        // Header Packet returns ERR
        $errReader = Mockery::mock(PayloadReader::class);
        $errReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFF);
        $errReader->shouldReceive('readFixedInteger')->with(2)->andReturn(1064);
        $errReader->shouldReceive('readFixedString')->with(1)->andReturn('#');
        $errReader->shouldReceive('readFixedString')->with(5)->andReturn('42000');
        $errReader->shouldReceive('readRestOfPacketString')->andReturn('Syntax error');

        $handler->processPacket($errReader, 20, 0);

        Loop::run();

        expect($errorCalled)->toBeTrue();
    });

    it('handles multiple rows in buffered mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT id FROM users', $promise);

        // Header Packet
        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $handler->processPacket($headerReader, 1, 0);

        // Column Definition Packet
        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(3, 3, 0);
        $colReader->shouldReceive('readFixedString')->with(3)->andReturn('def');
        $colReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn('db', 'table', 'orgTable', 'id', 'id');
        $colReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(12);
        $colReader->shouldReceive('readFixedInteger')->with(2)->andReturn(63, 0, 0);
        $colReader->shouldReceive('readFixedInteger')->with(4)->andReturn(11);
        $handler->processPacket($colReader, 20, 1);

        // INTERMEDIATE EOF PACKET
        $intermediateEofReader = Mockery::mock(PayloadReader::class);
        $intermediateEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $intermediateEofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($intermediateEofReader, 5, 2);

        // Row 1
        $rowReader1 = Mockery::mock(PayloadReader::class);
        $rowReader1->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $rowReader1->shouldReceive('readFixedString')->with(1)->andReturn('1');
        $handler->processPacket($rowReader1, 2, 3);

        // Row 2
        $rowReader2 = Mockery::mock(PayloadReader::class);
        $rowReader2->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $rowReader2->shouldReceive('readFixedString')->with(1)->andReturn('2');
        $handler->processPacket($rowReader2, 2, 4);

        // Final EOF Packet
        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($eofReader, 5, 5);

        $result = null;
        $promise->then(function ($r) use (&$result) {
            $result = $r;
        });

        Loop::run();

        expect($result)->toBeInstanceOf(Result::class)
            ->and($result->rowCount())->toBe(2)
            ->and($result->fetchAll())->toBe([['id' => '1'], ['id' => '2']])
        ;
    });
});
