<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Mysql\Handlers\QueryHandler;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
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

        $handler->start('INSERT INTO test VALUES (1)', $promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x00);
        $payloadReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1, 123, 0);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0);
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn('');

        $handler->processPacket($payloadReader, 7, 1);

        $resolved = false;
        $result = null;

        $promise->then(function ($r) use (&$resolved, &$result) {
            $resolved = true;
            $result = $r;
        });

        Loop::run();

        expect($resolved)->toBeTrue()
            ->and($result)->toBeInstanceOf(Result::class)
            ->and($result->getAffectedRows())->toBe(1)
            ->and($result->getLastInsertId())->toBe(123);
    });

    it('rejects promise on ERR packet', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT * FROM nonexistent', $promise);

        $payloadReader = Mockery::mock(PayloadReader::class);
        $payloadReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFF);
        $payloadReader->shouldReceive('readFixedInteger')->with(2)->andReturn(1146);
        $payloadReader->shouldReceive('readFixedString')->with(1)->andReturn('#');
        $payloadReader->shouldReceive('readFixedString')->with(5)->andReturn('42S02');
        $payloadReader->shouldReceive('readRestOfPacketString')->andReturn("Table 'test.nonexistent' doesn't exist");

        $handler->processPacket($payloadReader, 50, 1);

        $rejected = false;
        $errorMessage = '';

        $promise->catch(function ($e) use (&$rejected, &$errorMessage) {
            $rejected = true;
            $errorMessage = $e->getMessage();
        });

        Loop::run();

        expect($rejected)->toBeTrue()
            ->and($errorMessage)->toContain('MySQL Error')
            ->and($errorMessage)->toContain('1146');
    });

    it('handles result set with columns and rows in buffered mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT id, name FROM users', $promise);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(2);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(2);
        $handler->processPacket($headerReader, 1, 1);

        $col1Reader = Mockery::mock(PayloadReader::class);
        $col1Reader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x03);
        $col1Reader->shouldReceive('readFixedString')->with(3)->andReturn('def');
        $col1Reader->shouldReceive('readLengthEncodedStringOrNull')->andReturn(null, null, null, 'id');
        $col1Reader->shouldReceive('readRestOfPacketString')->andReturn('');
        $handler->processPacket($col1Reader, 20, 2);

        $col2Reader = Mockery::mock(PayloadReader::class);
        $col2Reader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x03);
        $col2Reader->shouldReceive('readFixedString')->with(3)->andReturn('def');
        $col2Reader->shouldReceive('readLengthEncodedStringOrNull')->andReturn(null, null, null, 'name');
        $col2Reader->shouldReceive('readRestOfPacketString')->andReturn('');
        $handler->processPacket($col2Reader, 20, 3);

        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedString')->with(4)->andReturn('');
        $handler->processPacket($eofReader, 5, 4);

        $rowReader = Mockery::mock(PayloadReader::class);
        $rowReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $rowReader->shouldReceive('readFixedString')->with(1)->andReturn('1');
        $rowReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn('Alice');
        $handler->processPacket($rowReader, 10, 5);

        $finalEofReader = Mockery::mock(PayloadReader::class);
        $finalEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $finalEofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($finalEofReader, 5, 6);

        $resolved = false;
        $result = null;

        $promise->then(function ($r) use (&$resolved, &$result) {
            $resolved = true;
            $result = $r;
        });

        Loop::run();

        expect($resolved)->toBeTrue()
            ->and($result)->toBeInstanceOf(Result::class);
    });

    it('handles streaming mode with onRow callback', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $receivedRows = [];
        $streamContext = new StreamContext(
            onRow: function (array $row) use (&$receivedRows) {
                $receivedRows[] = $row;
            }
        );

        $handler->start('SELECT id FROM test', $promise, $streamContext);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $handler->processPacket($headerReader, 1, 1);

        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x03);
        $colReader->shouldReceive('readFixedString')->with(3)->andReturn('def');
        $colReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn(null, null, null, 'id');
        $colReader->shouldReceive('readRestOfPacketString')->andReturn('');
        $handler->processPacket($colReader, 20, 2);

        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedString')->with(3)->andReturn('');
        $handler->processPacket($eofReader, 4, 3);

        $rowReader = Mockery::mock(PayloadReader::class);
        $rowReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $rowReader->shouldReceive('readFixedString')->with(1)->andReturn('1');
        $handler->processPacket($rowReader, 5, 4);

        $finalEofReader = Mockery::mock(PayloadReader::class);
        $finalEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $finalEofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($finalEofReader, 4, 5);

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
            ->and($receivedRows)->toHaveCount(1);
    });

    it('calls onComplete callback in streaming mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $completeCalled = false;
        $streamContext = new StreamContext(
            onRow: function (array $row) {},
            onComplete: function (StreamStats $stats) use (&$completeCalled) {
                $completeCalled = true;
                expect($stats->rowCount)->toBeGreaterThanOrEqual(0);
            }
        );

        $handler->start('SELECT 1', $promise, $streamContext);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $handler->processPacket($headerReader, 1, 1);

        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $colReader->shouldReceive('readFixedString')->with(0)->andReturn('');
        $handler->processPacket($colReader, 1, 2);

        $eofReader = Mockery::mock(PayloadReader::class);
        $eofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $eofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($eofReader, 3, 3);

        Loop::run();

        expect($completeCalled)->toBeTrue();
    });

    it('calls onError callback when exception occurs in streaming mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $errorCalled = false;
        $streamContext = new StreamContext(
            onRow: function (array $row) {
                throw new \RuntimeException('Row processing error');
            },
            onError: function (\Throwable $e) use (&$errorCalled) {
                $errorCalled = true;
                expect($e->getMessage())->toContain('Row processing error');
            }
        );

        $handler->start('SELECT 1', $promise, $streamContext);

        $promise->catch(function (\Throwable $e) {
        });

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $handler->processPacket($headerReader, 1, 1);

        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $colReader->shouldReceive('readFixedString')->with(0)->andReturn('');
        $handler->processPacket($colReader, 1, 2);

        $rowReader = Mockery::mock(PayloadReader::class);
        $rowReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $rowReader->shouldReceive('readFixedString')->with(1)->andReturn('1');
        $rowReader->shouldReceive('readRestOfPacketString')->andReturn('');

        try {
            $handler->processPacket($rowReader, 5, 3);
        } catch (\Throwable $e) {
        }

        Loop::run();

        expect($errorCalled)->toBeTrue();
    });

    it('handles multiple rows in buffered mode', function () {
        $socket = Mockery::mock(SocketConnection::class);
        $socket->shouldReceive('write')->once();

        $commandBuilder = new CommandBuilder();
        $handler = new QueryHandler($socket, $commandBuilder);
        $promise = new Promise();

        $handler->start('SELECT id FROM users LIMIT 3', $promise);

        $headerReader = Mockery::mock(PayloadReader::class);
        $headerReader->shouldReceive('readFixedInteger')->with(1)->andReturn(1);
        $headerReader->shouldReceive('readLengthEncodedIntegerOrNull')->andReturn(1);
        $handler->processPacket($headerReader, 1, 1);

        $colReader = Mockery::mock(PayloadReader::class);
        $colReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x02);
        $colReader->shouldReceive('readFixedString')->with(2)->andReturn('de');
        $colReader->shouldReceive('readLengthEncodedStringOrNull')->andReturn(null, null, null, 'id');
        $colReader->shouldReceive('readRestOfPacketString')->andReturn('');
        $handler->processPacket($colReader, 20, 2);

        $colEofReader = Mockery::mock(PayloadReader::class);
        $colEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $colEofReader->shouldReceive('readFixedString')->with(0)->andReturn('');
        $handler->processPacket($colEofReader, 1, 3);

        $row1Reader = Mockery::mock(PayloadReader::class);
        $row1Reader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $row1Reader->shouldReceive('readFixedString')->with(1)->andReturn('1');
        $handler->processPacket($row1Reader, 5, 4);

        $row2Reader = Mockery::mock(PayloadReader::class);
        $row2Reader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $row2Reader->shouldReceive('readFixedString')->with(1)->andReturn('2');
        $handler->processPacket($row2Reader, 5, 5);

        $row3Reader = Mockery::mock(PayloadReader::class);
        $row3Reader->shouldReceive('readFixedInteger')->with(1)->andReturn(0x01);
        $row3Reader->shouldReceive('readFixedString')->with(1)->andReturn('3');
        $handler->processPacket($row3Reader, 5, 6);

        $finalEofReader = Mockery::mock(PayloadReader::class);
        $finalEofReader->shouldReceive('readFixedInteger')->with(1)->andReturn(0xFE);
        $finalEofReader->shouldReceive('readFixedInteger')->with(2)->andReturn(0, 0);
        $handler->processPacket($finalEofReader, 4, 7);

        $result = null;
        $promise->then(function ($r) use (&$result) {
            $result = $r;
        });

        Loop::run();

        expect($result)->toBeInstanceOf(Result::class);
    });
});