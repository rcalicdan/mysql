<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Enums\ParserState;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Constants\LengthEncodedType;
use Rcalicdan\MySQLBinaryProtocol\Constants\StatusFlags;
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

/**
 * Handles text protocol query execution (COM_QUERY).
 *
 * Supports both buffered and streaming modes:
 * - Buffered: All rows loaded into Result object
 * - Streaming: Rows delivered via callback with StreamStats result
 *
 * @internal
 * @package Hibla\Mysql\Handlers
 */
final class QueryHandler
{
    private array $columns = [];
    private array $rows = [];
    private int $columnCount = 0;
    private int $sequenceId = 0;
    private int $streamedRowCount = 0;
    private float $streamStartTime = 0;
    private bool $isDraining = false;
    private readonly ResponseParser $responseParser;
    private readonly ColumnDefinitionParser $columnParser;
    private ParserState $state = ParserState::INIT;
    private ?RowOrEofParser $rowParser = null;
    private ?StreamContext $streamContext = null;
    private ?Promise $currentPromise = null;
    private ?Result $primaryResult = null;
    private ?StreamStats $primaryStreamStats = null;

    public function __construct(
        private readonly SocketConnection $socket,
        private readonly CommandBuilder $commandBuilder
    ) {
        $this->responseParser = new ResponseParser();
        $this->columnParser = new ColumnDefinitionParser();
    }

    public function start(string $sql, Promise $promise, ?StreamContext $streamContext = null): void
    {
        $this->state = ParserState::INIT;
        $this->columnCount = 0;
        $this->columns = [];
        $this->rows = [];
        $this->currentPromise = $promise;
        $this->sequenceId = 0;
        $this->rowParser = null;
        $this->streamContext = $streamContext;
        $this->streamedRowCount = 0;
        $this->streamStartTime = microtime(true);
        $this->primaryResult = null;
        $this->primaryStreamStats = null;
        $this->isDraining = false;

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
            if ($this->streamContext !== null && $this->streamContext->onError !== null) {
                try {
                    ($this->streamContext->onError)($e);
                } catch (\Throwable $callbackError) {
                }
            }

            $this->currentPromise?->reject($e);

            try {
                $reader->readRestOfPacketString();
            } catch (\Throwable $t) {
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
            $result = new Result(
                rows: [],
                affectedRows: $frame->affectedRows,
                lastInsertId: $frame->lastInsertId,
                warningCount: $frame->warnings
            );

            if ($this->hasMoreResults($frame->statusFlags)) {
                $this->prepareDrain($result, null);

                return;
            }

            $finalResult = $this->primaryResult ?? $result;
            $this->currentPromise?->resolve($finalResult);

            return;
        }

        if ($frame instanceof ResultSetHeader) {
            $this->columnCount = $frame->columnCount;
            $this->state = ParserState::COLUMNS;

            return;
        }

        throw new \RuntimeException('Unexpected packet type in header state');
    }

    private function hasMoreResults(int $flags): bool
    {
        return ($flags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) !== 0;
    }

    private function prepareDrain(?Result $result, ?StreamStats $stats): void
    {
        if (! $this->isDraining) {
            $this->primaryResult = $result;
            $this->primaryStreamStats = $stats;
            $this->isDraining = true;
        }

        $this->state = ParserState::INIT;
        $this->columnCount = 0;
        $this->columns = [];
        $this->rows = [];
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

        if ($this->isDraining) {
            $reader->readRestOfPacketString();

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

    private function handleRow(PayloadReader $reader, int $length, int $seq): void
    {
        if ($this->rowParser === null) {
            throw new \RuntimeException('Row parser not initialized');
        }

        $frame = $this->rowParser->parse($reader, $length, $seq);

        if ($frame instanceof ErrPacket) {
            $this->handleRowError($frame);

            return;
        }

        if ($frame instanceof EofPacket) {
            $this->handleEndOfResultSet($frame);

            return;
        }

        if ($frame instanceof TextRow) {
            $this->handleTextRow($frame);
        }
    }

    private function handleRowError(ErrPacket $packet): void
    {
        $exception = new \RuntimeException(
            "MySQL Error [{$packet->errorCode}]: {$packet->errorMessage}"
        );

        if ($this->streamContext?->onError !== null) {
            try {
                ($this->streamContext->onError)($exception);
            } catch (\Throwable $e) {
            }
        }

        $this->currentPromise?->reject($exception);
    }

    private function handleEndOfResultSet(EofPacket $packet): void
    {
        if ($this->streamContext !== null) {
            $stats = new StreamStats(
                rowCount: $this->streamedRowCount,
                columnCount: \count($this->columns),
                duration: microtime(true) - $this->streamStartTime,
                warningCount: $packet->warnings
            );
            $currentResult = null;
            $currentStats = $stats;
        } else {
            $result = new Result(
                rows: $this->rows,
                affectedRows: 0,
                lastInsertId: 0,
                warningCount: $packet->warnings,
                columns: $this->columns
            );
            $currentResult = $result;
            $currentStats = null;
        }

        if ($this->hasMoreResults($packet->statusFlags)) {
            $this->prepareDrain($currentResult, $currentStats);

            return;
        }

        if ($this->streamContext !== null) {
            $finalStats = $this->primaryStreamStats ?? $currentStats;
            if ($this->streamContext->onComplete !== null) {
                try {
                    ($this->streamContext->onComplete)($finalStats);
                } catch (\Throwable $e) {
                }
            }
            $this->currentPromise?->resolve($finalStats);
        } else {
            $finalResult = $this->primaryResult ?? $currentResult;
            $this->currentPromise?->resolve($finalResult);
        }
    }

    private function handleTextRow(TextRow $row): void
    {
        if ($this->isDraining) {
            return;
        }

        $assocRow = $this->convertRowToAssociativeArray($row);

        if ($this->streamContext !== null) {
            $this->processStreamingRow($assocRow);
        } else {
            $this->rows[] = $assocRow;
        }
    }

    private function convertRowToAssociativeArray(TextRow $row): array
    {
        $assocRow = [];
        $nameCounts = [];

        foreach ($row->values as $index => $value) {
            $colName = $this->columns[$index] ?? (string)$index;

            if (isset($nameCounts[$colName])) {
                $suffix = $nameCounts[$colName]++;
                $colName .= $suffix;
            } else {
                $nameCounts[$colName] = 1;
            }

            $assocRow[$colName] = $value;
        }

        return $assocRow;
    }

    private function processStreamingRow(array $row): void
    {
        try {
            ($this->streamContext->onRow)($row);
            $this->streamedRowCount++;
        } catch (\Throwable $e) {
            if ($this->streamContext->onError !== null) {
                ($this->streamContext->onError)($e);
            }

            throw $e;
        }
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

    private function writePacket(string $payload): void
    {
        $len = \strlen($payload);
        $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
        $this->socket->write($header . $payload);
        $this->sequenceId++;
    }
}
