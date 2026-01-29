<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Enums\ParserState;
use Hibla\Mysql\Internals\ExecuteResult;
use Hibla\Mysql\Internals\QueryResult;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
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

/**
 * Handles text protocol query execution (COM_QUERY).
 *
 * Supports both buffered and streaming modes:
 * - Buffered: All rows loaded into QueryResult
 * - Streaming: Rows delivered via callback with StreamStats result
 *
 * @internal
 * @package Hibla\Mysql\Handlers
 */
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
    private ?StreamContext $streamContext = null;
    private int $streamedRowCount = 0;
    private float $streamStartTime = 0;

    public function __construct(
        private readonly SocketConnection $socket,
        private readonly CommandBuilder $commandBuilder
    ) {
        $this->responseParser = new ResponseParser();
        $this->columnParser = new ColumnDefinitionParser();
    }

    /**
     * Start a query operation.
     *
     * @param string $sql The SQL query to execute
     * @param Promise $promise Promise to resolve with result
     * @param StreamContext|null $streamContext Optional streaming context for memory-efficient processing
     */
    public function start(string $sql, Promise $promise, ?StreamContext $streamContext = null): void
    {
        $this->state = ParserState::INIT;
        $this->columnCount = 0;
        $this->columns = [];
        $this->rows = [];
        $this->currentPromise = $promise;
        $this->sequenceId = 0;
        $this->rowParser = null;
        // Streaming setup
        $this->streamContext = $streamContext;
        $this->streamedRowCount = 0;
        $this->streamStartTime = microtime(true);

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
            // Notify stream error callback if streaming
            if ($this->streamContext !== null && $this->streamContext->onError !== null) {
                try {
                    ($this->streamContext->onError)($e);
                } catch (\Throwable $callbackError) {
                    // Ignore callback errors
                }
            }

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
            // Query complete - return appropriate result
            if ($this->streamContext !== null) {
                // Streaming mode: return statistics
                $stats = new StreamStats(
                    rowCount: $this->streamedRowCount,
                    columnCount: \count($this->columns),
                    duration: microtime(true) - $this->streamStartTime
                );

                // Notify completion callback
                if ($this->streamContext->onComplete !== null) {
                    try {
                        ($this->streamContext->onComplete)($stats);
                    } catch (\Throwable $e) {
                        // Ignore callback errors
                    }
                }

                $this->currentPromise?->resolve($stats);
            } else {
                // Buffered mode: return QueryResult
                $result = new QueryResult($this->rows);
                $this->currentPromise?->resolve($result);
            }

            return;
        }

        if ($frame instanceof TextRow) {
            $assocRow = [];
            foreach ($frame->values as $index => $value) {
                $colName = $this->columns[$index] ?? $index;
                $assocRow[$colName] = $value;
            }

            if ($this->streamContext !== null) {
                // Streaming mode: deliver row via callback
                try {
                    ($this->streamContext->onRow)($assocRow);
                    $this->streamedRowCount++;
                } catch (\Throwable $e) {
                    // Notify error callback
                    if ($this->streamContext->onError !== null) {
                        ($this->streamContext->onError)($e);
                    }

                    throw $e;
                }
            } else {
                // Buffered mode: collect row
                $this->rows[] = $assocRow;
            }
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
