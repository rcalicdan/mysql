<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Handlers;

use Hibla\MysqlClient\Enums\ExecuteState;
use Hibla\MysqlClient\Internals\ExecuteResult;
use Hibla\MysqlClient\Internals\QueryResult;
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
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class ExecuteHandler
{
    private ExecuteState $state = ExecuteState::HEADER;
    private int $sequenceId = 0;
    private ?Promise $currentPromise = null;
    private array $rows = [];
    private array $columnDefinitions = [];

    public function __construct(
        private readonly SocketConnection $socket,
        private readonly CommandBuilder $commandBuilder
    ) {
    }

    public function start(int $stmtId, array $params, array $columnDefinitions, Promise $promise): void
    {
        $this->state = ExecuteState::HEADER;
        $this->rows = [];
        $this->columnDefinitions = $columnDefinitions;
        $this->currentPromise = $promise;
        $this->sequenceId = 0;

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

        $reader->readFixedString((int)$firstByte);
        $reader->readRestOfPacketString();

        return false;
    }

    private function handleRow(PayloadReader $reader, int $length): bool
    {
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === PacketType::EOF && $length < PacketType::EOF_MAX_LENGTH) {
            if ($length > 1) {
                $reader->readFixedString($length - 1);
            }

            $result = new QueryResult($this->rows);
            $this->currentPromise?->resolve($result);

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
            if ($this->isColumnNull($nullBitmap, $i)) {
                $values[] = null;

                continue;
            }
            $values[] = $this->readBinaryValue($reader, $column->type);
        }

        $assocRow = [];
        foreach ($values as $index => $val) {
            $colName = $this->columnDefinitions[$index]->name;
            $assocRow[$colName] = $val;
        }
        $this->rows[] = $assocRow;

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

    /**
     * Read a binary-encoded value based on MySQL type.
     *
     * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value
     */
    private function readBinaryValue(PayloadReader $reader, int $type): mixed
    {
        return match ($type) {
            MysqlType::TINY => $reader->readFixedInteger(1),
            MysqlType::SHORT, MysqlType::YEAR => $reader->readFixedInteger(2),
            MysqlType::LONG, MysqlType::INT24 => $reader->readFixedInteger(4),
            MysqlType::LONGLONG => $reader->readFixedInteger(8),
            MysqlType::FLOAT => unpack('f', $reader->readFixedString(4))[1],
            MysqlType::DOUBLE => unpack('d', $reader->readFixedString(8))[1],
            MysqlType::DATE,
            MysqlType::DATETIME,
            MysqlType::TIMESTAMP => $this->readBinaryDateTime($reader, $type),
            MysqlType::TIME => $this->readBinaryTime($reader),
            default => $reader->readLengthEncodedStringOrNull()
        };
    }

    /**
     * Read binary-encoded DATE, DATETIME, or TIMESTAMP.
     *
     * Binary format according to MySQL protocol specification:
     * - 1 byte: length indicator
     *   - 0: All fields are zero (zero date: '0000-00-00' or '0000-00-00 00:00:00')
     *   - 4: Only year, month, day (no time component)
     *   - 7: Year, month, day, hour, minute, second (no microseconds)
     *   - 11: Year, month, day, hour, minute, second, microseconds (full precision)
     * - If length >= 4: 2 bytes year, 1 byte month, 1 byte day
     * - If length >= 7: 1 byte hour, 1 byte minute, 1 byte second
     * - If length = 11: 4 bytes microseconds (little-endian)
     *
     * @param PayloadReader $reader The payload reader
     * @param int $type The MySQL type (DATE, DATETIME, or TIMESTAMP)
     * @return string|null The formatted date/time string or null
     *
     * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
     */
    private function readBinaryDateTime(PayloadReader $reader, int $type): ?string
    {
        $length = $reader->readFixedInteger(1);

        if ($length === 0) {
            return $type === MysqlType::DATE
                ? '0000-00-00'
                : '0000-00-00 00:00:00';
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
        }

        return $date;
    }

    /**
     * Read binary-encoded TIME value.
     *
     * Binary format according to MySQL protocol specification:
     * - 1 byte: length indicator (0, 8, or 12)
     * - If length >= 8:
     *   - 1 byte: is_negative (1 if minus, 0 for plus)
     *   - 4 bytes: days
     *   - 1 byte: hour
     *   - 1 byte: minute
     *   - 1 byte: second
     * - If length = 12:
     *   - 4 bytes: microseconds
     *
     * Note: TIME can represent up to ±838:59:59 (not just 0-23 hours)
     * This is because TIME can store elapsed time or intervals, not just time-of-day.
     *
     * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
     */
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
}
