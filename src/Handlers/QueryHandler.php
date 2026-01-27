<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Handlers;

use Hibla\MysqlClient\Enums\ParserState;
use Hibla\MysqlClient\ValueObjects\ExecuteResult;
use Hibla\MysqlClient\ValueObjects\QueryResult;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Constants\LengthEncodedType;
use Rcalicdan\MySQLBinaryProtocol\Exception\IncompleteBufferException;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\EofPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResponseParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResultSetHeader;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\RowOrEofParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinitionParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\TextRow;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class QueryHandler
{
    private ParserState $state = ParserState::INIT;
    private int $columnCount = 0;
    private array $columns = [];
    private array $rows = [];

    private ?Promise $currentPromise = null;
    private int $sequenceId = 0;

    private readonly ResponseParser $responseParser;
    private readonly ColumnDefinitionParser $columnParser;
    private ?RowOrEofParser $rowParser = null;

    public function __construct(
        private readonly SocketConnection $socket,
        private readonly CommandBuilder $commandBuilder
    ) {
        $this->responseParser = new ResponseParser();
        $this->columnParser = new ColumnDefinitionParser();
    }

    public function start(string $sql, Promise $promise): void
    {
        $this->state = ParserState::INIT;
        $this->columnCount = 0;
        $this->columns = [];
        $this->rows = [];
        $this->currentPromise = $promise;
        $this->sequenceId = 0;
        $this->rowParser = null;

        $packet = $this->commandBuilder->buildQuery($sql);
        $this->writePacket($packet);
    }

    public function processPacket(PayloadReader $reader, int $length, int $seq): void
    {
        try {
            $this->sequenceId = $seq + 1;

            match ($this->state) {
                ParserState::INIT => $this->handleHeader($reader, $length, $seq),
                ParserState::COLUMNS => $this->handleColumn($reader, $length, $seq),
                ParserState::ROWS => $this->handleRow($reader, $length, $seq),
            };
        } catch (IncompleteBufferException $e) {
            throw $e;
        } catch (\Throwable $e) {
            $this->currentPromise?->reject($e);

            // Drain to avoid loop
            try {
                $reader->readRestOfPacketString();
            } catch (\Throwable $t) {
                // Ignore
            }
        }
    }

    private function handleHeader(PayloadReader $reader, int $length, int $seq): void
    {
        $frame = $this->responseParser->parseResponse($reader, $length, $seq);

        if ($frame instanceof ErrPacket) {
            $this->currentPromise?->reject(
                new \RuntimeException("MySQL Error [{$frame->errorCode}]: {$frame->errorMessage}")
            );

            return;
        }

        if ($frame instanceof OkPacket) {
            // For execute() operations
            $result = new ExecuteResult(
                affectedRows: $frame->affectedRows,
                lastInsertId: $frame->lastInsertId,
                warningCount: $frame->warningCount ?? 0
            );
            $this->currentPromise?->resolve($result);

            return;
        }

        if ($frame instanceof ResultSetHeader) {
            // For query() operations
            $this->columnCount = $frame->columnCount;
            $this->state = ParserState::COLUMNS;

            return;
        }

        throw new \RuntimeException('Unexpected packet type in header state');
    }

    private function handleColumn(PayloadReader $reader, int $length, int $seq): void
    {
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === 0xFE && $length < 9) {
            if ($length > 1) {
                $reader->readFixedString($length - 1);
            }
            $this->state = ParserState::ROWS;
            $this->rowParser = new RowOrEofParser($this->columnCount);

            return;
        }

        $this->readLengthEncodedStringWithFirstByte($reader, (int)$firstByte);

        $reader->readLengthEncodedStringOrNull();

        $reader->readLengthEncodedStringOrNull();

        $reader->readLengthEncodedStringOrNull();

        $name = $reader->readLengthEncodedStringOrNull();

        $reader->readRestOfPacketString();

        $this->columns[] = $name ?? 'unknown';
    }

    private function readLengthEncodedStringWithFirstByte(PayloadReader $reader, int $firstByte): ?string
    {
        return match ($firstByte) {
            LengthEncodedType::NULL_MARKER => null,
            LengthEncodedType::INT16_LENGTH => $reader->readFixedString((int)$reader->readFixedInteger(2)),
            LengthEncodedType::INT24_LENGTH => $reader->readFixedString((int)$reader->readFixedInteger(3)),
            LengthEncodedType::INT64_LENGTH => $reader->readFixedString((int)$reader->readFixedInteger(8)),
            default => $firstByte < LengthEncodedType::NULL_MARKER
                ? $reader->readFixedString($firstByte)
                : throw new \RuntimeException(
                    \sprintf('Invalid length-encoded string marker: 0x%02X', $firstByte)
                ),
        };
    }

    private function handleRow(PayloadReader $reader, int $length, int $seq): void
    {
        if ($this->rowParser === null) {
            throw new \RuntimeException('Row parser not initialized');
        }

        $frame = $this->rowParser->parse($reader, $length, $seq);

        if ($frame instanceof EofPacket) {
            // Return QueryResult for SELECT queries
            $result = new QueryResult($this->rows);
            $this->currentPromise?->resolve($result);

            return;
        }

        if ($frame instanceof TextRow) {
            $assocRow = [];
            foreach ($frame->values as $index => $value) {
                $colName = $this->columns[$index] ?? $index;
                $assocRow[$colName] = $value;
            }
            $this->rows[] = $assocRow;
        }
    }

    private function writePacket(string $payload): void
    {
        $len = \strlen($payload);
        $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
        $this->socket->write($header . $payload);
        $this->sequenceId++;
    }
}
