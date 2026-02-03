<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Enums\ExecuteState;
use Hibla\Mysql\Internals\ExecuteResult;
use Hibla\Mysql\Internals\QueryResult;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Constants\MysqlType;
use Rcalicdan\MySQLBinaryProtocol\Constants\PacketType;
use Rcalicdan\MySQLBinaryProtocol\Exception\IncompleteBufferException;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResponseParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResultSetHeader;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Handles binary protocol prepared statement execution (COM_STMT_EXECUTE).
 *
 * Supports both buffered and streaming modes:
 * - Buffered: All rows loaded into QueryResult
 * - Streaming: Rows delivered via callback with StreamStats result
 *
 * @internal
 * @package Hibla\Mysql\Handlers
 */
final class ExecuteHandler
{
    private const UNSIGNED_FLAG = 32;

    private ExecuteState $state = ExecuteState::HEADER;
    private int $sequenceId = 0;
    private ?Promise $currentPromise = null;
    private array $rows = [];
    private array $columnDefinitions = [];
    private ?StreamContext $streamContext = null;
    private int $streamedRowCount = 0;
    private float $streamStartTime = 0;
    private bool $receivedNewMetadata = false;

    public function __construct(
        private readonly SocketConnection $socket,
        private readonly CommandBuilder $commandBuilder
    ) {
    }

    public function start(
        int $stmtId,
        array $params,
        array $columnDefinitions,
        Promise $promise,
        ?StreamContext $streamContext = null
    ): void {
        $this->state = ExecuteState::HEADER;
        $this->rows = [];
        $this->columnDefinitions = $columnDefinitions;
        $this->currentPromise = $promise;
        $this->sequenceId = 0;
        $this->receivedNewMetadata = false;

        $this->streamContext = $streamContext;
        $this->streamedRowCount = 0;
        $this->streamStartTime = microtime(true);

        $packet = $this->commandBuilder->buildStmtExecute($stmtId, $params);
        $this->writePacket($packet);
    }

    public function processPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        try {
            $this->sequenceId = $seq + 1;

            return match ($this->state) {
                ExecuteState::HEADER => $this->handleHeader($reader, $length, $seq),
                ExecuteState::CHECK_DATA => $this->handleDataPacket($reader, $length),
                ExecuteState::ROWS => $this->handleRow($reader, $length),
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

            return true;
        }
    }

    private function handleHeader(PayloadReader $reader, int $length, int $seq): bool
    {
        $responseParser = new ResponseParser();
        $frame = $responseParser->parseResponse($reader, $length, $seq);

        if ($frame instanceof ErrPacket) {
            $this->currentPromise?->reject(new \RuntimeException("Execute Error: {$frame->errorMessage}"));
            return true;
        }

        if ($frame instanceof OkPacket) {
            $result = new ExecuteResult($frame->affectedRows, $frame->lastInsertId, $frame->warnings);
            $this->currentPromise?->resolve($result);
            return true;
        }

        if ($frame instanceof ResultSetHeader) {
            $this->state = ExecuteState::CHECK_DATA;
            return false;
        }

        throw new \RuntimeException('Unexpected packet in Execute Header');
    }

    private function handleDataPacket(PayloadReader $reader, int $length): bool
    {
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === PacketType::OK) {
            $this->state = ExecuteState::ROWS;
            return $this->parseRow($reader);
        }

        if ($firstByte === PacketType::EOF && $length < PacketType::EOF_MAX_LENGTH) {
            if ($length > 1) {
                $reader->readFixedString($length - 1);
            }
            $this->state = ExecuteState::ROWS;
            return false;
        }

        if (!$this->receivedNewMetadata) {
            $this->columnDefinitions = [];
            $this->receivedNewMetadata = true;
        }

        $catalog = $this->readStringGivenLength($reader, (int)$firstByte);
        $schema = $reader->readLengthEncodedStringOrNull() ?? '';
        $table = $reader->readLengthEncodedStringOrNull() ?? '';
        $orgTable = $reader->readLengthEncodedStringOrNull() ?? '';
        $name = $reader->readLengthEncodedStringOrNull() ?? '';
        $orgName = $reader->readLengthEncodedStringOrNull() ?? '';

        $reader->readLengthEncodedIntegerOrNull();

        $charset = (int)$reader->readFixedInteger(2);
        $colLength = (int)$reader->readFixedInteger(4);
        $type = (int)$reader->readFixedInteger(1);
        $flags = (int)$reader->readFixedInteger(2);
        $decimals = (int)$reader->readFixedInteger(1);

        $reader->readRestOfPacketString();

        $this->columnDefinitions[] = new ColumnDefinition(
            $catalog,
            $schema,
            $table,
            $orgTable,
            $name,
            $orgName,
            $charset,
            $colLength,
            $type,
            $flags,
            $decimals
        );

        return false;
    }

    private function handleRow(PayloadReader $reader, int $length): bool
    {
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === PacketType::EOF && $length < PacketType::EOF_MAX_LENGTH) {
            if ($length > 1) {
                $reader->readFixedString($length - 1);
            }

            if ($this->streamContext !== null) {
                $stats = new StreamStats(
                    rowCount: $this->streamedRowCount,
                    columnCount: \count($this->columnDefinitions),
                    duration: microtime(true) - $this->streamStartTime
                );

                if ($this->streamContext->onComplete !== null) {
                    try {
                        ($this->streamContext->onComplete)($stats);
                    } catch (\Throwable $e) {
                    }
                }

                $this->currentPromise?->resolve($stats);
            } else {
                $result = new QueryResult($this->rows);
                $this->currentPromise?->resolve($result);
            }

            return true;
        }

        if ($firstByte === PacketType::ERR) {
            $errorCode = $reader->readFixedInteger(2);
            $reader->readFixedString(1);
            $reader->readFixedString(5);
            $msg = $reader->readRestOfPacketString();
            
            $errorMsg = new \RuntimeException("MySQL Error [$errorCode]: $msg");

            if ($this->streamContext !== null && $this->streamContext->onError !== null) {
                try {
                    ($this->streamContext->onError)($errorMsg);
                } catch (\Throwable $e) {
                }
            }

            $this->currentPromise?->reject($errorMsg);
            return true;
        }

        if ($firstByte !== PacketType::OK) {
            throw new \RuntimeException('Invalid Binary Row');
        }

        return $this->parseRow($reader);
    }

    private function parseRow(PayloadReader $reader): bool
    {
        $columnCount = \count($this->columnDefinitions);
        $nullBitmapBytes = (int) floor(($columnCount + 9) / 8);
        $nullBitmap = $reader->readFixedString($nullBitmapBytes);

        $values = [];
        foreach ($this->columnDefinitions as $i => $column) {
            $isNull = $this->isColumnNull($nullBitmap, $i);

            if ($isNull) {
                $values[] = null;
                continue;
            }

            $values[] = $this->readBinaryValue($reader, $column);
        }

        $assocRow = [];
        $nameCounts = [];

        foreach ($values as $index => $val) {
            $colName = $this->columnDefinitions[$index]->name;

            if (isset($nameCounts[$colName])) {
                $suffix = $nameCounts[$colName]++;
                $colName = $colName . $suffix;
            } else {
                $nameCounts[$colName] = 1;
            }
            
            $assocRow[$colName] = $val;
        }

        if ($this->streamContext !== null) {
            try {
                ($this->streamContext->onRow)($assocRow);
                $this->streamedRowCount++;
            } catch (\Throwable $e) {
                if ($this->streamContext->onError !== null) {
                    ($this->streamContext->onError)($e);
                }
                throw $e;
            }
        } else {
            $this->rows[] = $assocRow;
        }

        return false;
    }

    private function isColumnNull(string $nullBitmap, int $index): bool
    {
        $bitPos = $index + 2;
        $byteIdx = (int) floor($bitPos / 8);
        $bit = (1 << ($bitPos % 8));

        if (! isset($nullBitmap[$byteIdx])) {
            return false;
        }

        return (\ord($nullBitmap[$byteIdx]) & $bit) !== 0;
    }

    private function readBinaryValue(PayloadReader $reader, ColumnDefinition $column): mixed
    {
        // Handle NEWDECIMAL (Type 246) explicitly as string
        if ($column->type === MysqlType::NEWDECIMAL) {
            return $reader->readLengthEncodedStringOrNull();
        }

        $val = match ($column->type) {
            MysqlType::TINY => $reader->readFixedInteger(1),
            MysqlType::SHORT, MysqlType::YEAR => $reader->readFixedInteger(2),
            MysqlType::LONG, MysqlType::INT24 => $reader->readFixedInteger(4),
            MysqlType::LONGLONG => $reader->readFixedInteger(8),
            MysqlType::FLOAT => unpack('f', $reader->readFixedString(4))[1],
            MysqlType::DOUBLE => unpack('d', $reader->readFixedString(8))[1],
            MysqlType::DATE,
            MysqlType::DATETIME,
            MysqlType::TIMESTAMP => $this->readBinaryDateTime($reader, $column->type),
            MysqlType::TIME => $this->readBinaryTime($reader),
            default => $reader->readLengthEncodedStringOrNull()
        };

        // Handle Signed Integers
        // Only apply to actual integer types. Do NOT cast FLOATS to ints here.
        if (in_array($column->type, [MysqlType::TINY, MysqlType::SHORT, MysqlType::INT24, MysqlType::LONG, MysqlType::LONGLONG], true) 
            && !($column->flags & self::UNSIGNED_FLAG)) {
            
            switch ($column->type) {
                case MysqlType::TINY:
                    $val = (int)$val;
                    if ($val >= 128) $val -= 256;
                    break;
                case MysqlType::SHORT:
                case MysqlType::YEAR:
                    $val = (int)$val;
                    if ($val >= 32768) $val -= 65536;
                    break;
                case MysqlType::INT24:
                    $val = (int)$val;
                    if ($val >= 8388608) $val -= 16777216;
                    break;
                case MysqlType::LONG:
                    $val = (int)$val;
                    if ($val >= 2147483648) $val -= 4294967296;
                    break;
                case MysqlType::LONGLONG:
                    if ($val > 9223372036854775807) {
                        $val = (int)($val - 18446744073709551616);
                    } else {
                        $val = (int)$val;
                    }
                    break;
            }
        }

        // Handle Unsigned LONGLONG
        if ($column->type === MysqlType::LONGLONG && ($column->flags & self::UNSIGNED_FLAG)) {
            // If it's a float (exceeds PHP_INT_MAX), convert to string
            if (is_float($val)) {
                return number_format($val, 0, '', '');
            }
        }

        return $val;
    }

    private function readBinaryDateTime(PayloadReader $reader, int $type): ?string
    {
        $length = $reader->readFixedInteger(1);

        if ($length === 0) {
            if ($type === MysqlType::DATE) {
                return '0000-00-00';
            }
            return '0000-00-00 00:00:00';
        }

        $year = $reader->readFixedInteger(2);
        $month = $reader->readFixedInteger(1);
        $day = $reader->readFixedInteger(1);

        $date = \sprintf('%04d-%02d-%02d', $year, $month, $day);

        if ($type === MysqlType::DATE) {
            return $date;
        }

        if ($length >= 7) {
            $hour = $reader->readFixedInteger(1);
            $min = $reader->readFixedInteger(1);
            $sec = $reader->readFixedInteger(1);
            $date .= \sprintf(' %02d:%02d:%02d', $hour, $min, $sec);

            if ($length === 11) {
                $microseconds = $reader->readFixedInteger(4);
                $date .= \sprintf('.%06d', $microseconds);
            }
        } else {
            // Length was 4 (Date only), but type is DATETIME/TIMESTAMP
            $date .= ' 00:00:00';
        }

        return $date;
    }

    private function readBinaryTime(PayloadReader $reader): ?string
    {
        $length = $reader->readFixedInteger(1);

        if ($length === 0) {
            return '00:00:00';
        }

        $isNegative = $reader->readFixedInteger(1);
        $days = $reader->readFixedInteger(4);
        $hour = $reader->readFixedInteger(1);
        $min = $reader->readFixedInteger(1);
        $sec = $reader->readFixedInteger(1);

        $totalHours = $days * 24 + $hour;

        $time = \sprintf(
            '%s%02d:%02d:%02d',
            $isNegative ? '-' : '',
            $totalHours,
            $min,
            $sec
        );

        if ($length === 12) {
            $microseconds = $reader->readFixedInteger(4);
            $time .= \sprintf('.%06d', $microseconds);
        }

        return $time;
    }

    private function writePacket(string $payload): void
    {
        $len = \strlen($payload);
        $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
        $this->socket->write($header . $payload);
        $this->sequenceId++;
    }

    private function readStringGivenLength(PayloadReader $reader, int $firstByte): string 
    {
        if ($firstByte < 251) {
            return $reader->readFixedString($firstByte);
        }
        
        if ($firstByte === 0xfc) {
            $len = $reader->readFixedInteger(2);
            return $reader->readFixedString((int)$len);
        }
        if ($firstByte === 0xfd) {
            $len = $reader->readFixedInteger(3);
            return $reader->readFixedString((int)$len);
        }
        if ($firstByte === 0xfe) {
            $len = $reader->readFixedInteger(8);
            return $reader->readFixedString((int)$len);
        }

        return '';
    }
}