<?php

namespace Hibla\MysqlClient\Protocols;

use Hibla\MysqlClient\Result;
use Rcalicdan\MySQLBinaryProtocol\Buffer\Reader\BufferPayloadReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Parses a result set that is returned in the Binary Row Protocol format.
 * This is used for the results of prepared statements.
 */
final class BinaryResultSetParser
{
    private const STATE_INIT = 0;
    private const STATE_COLUMNS = 1;
    private const STATE_ROWS = 2;

    // MySQL field types
    private const FIELD_TYPE_TINY = 0x01;
    private const FIELD_TYPE_SHORT = 0x02;
    private const FIELD_TYPE_LONG = 0x03;
    private const FIELD_TYPE_FLOAT = 0x04;
    private const FIELD_TYPE_DOUBLE = 0x05;
    private const FIELD_TYPE_TIMESTAMP = 0x07;
    private const FIELD_TYPE_LONGLONG = 0x08;
    private const FIELD_TYPE_MEDIUM = 0x09;
    private const FIELD_TYPE_DATE = 0x0A;
    private const FIELD_TYPE_TIME = 0x0B;
    private const FIELD_TYPE_DATETIME = 0x0C;

    private const EOF_MARKER = 0xFE;
    private const EOF_PACKET_MAX_LENGTH = 9;

    private int $state = self::STATE_INIT;
    private ?int $columnCount = null;
    private array $columns = [];
    private array $rows = [];
    private bool $isComplete = false;
    private ?BufferPayloadReaderFactory $readerFactory = null;

    public function processPayload(string $rawPayload): void
    {
        if ($this->isComplete) {
            return;
        }

        $reader = $this->getReader($rawPayload);
        $firstByte = ord($rawPayload[0]);

        if ($this->isEndOfResultSet($firstByte, $rawPayload)) {
            $this->markComplete();

            return;
        }

        match ($this->state) {
            self::STATE_INIT => $this->handleInitState($reader),
            self::STATE_COLUMNS => $this->handleColumnsState($reader, $firstByte),
            self::STATE_ROWS => $this->handleRowsState($reader),
        };
    }

    private function getReader(string $rawPayload): PayloadReader
    {
        $this->readerFactory ??= new BufferPayloadReaderFactory;

        return $this->readerFactory->createFromString($rawPayload);
    }

    private function isEndOfResultSet(int $firstByte, string $rawPayload): bool
    {
        return $this->state === self::STATE_ROWS
            && $firstByte === self::EOF_MARKER
            && strlen($rawPayload) < self::EOF_PACKET_MAX_LENGTH;
    }

    private function markComplete(): void
    {
        $this->isComplete = true;
        // Clear intermediate data to free memory
        $this->columns = [];
        $this->readerFactory = null;
    }

    private function handleInitState(PayloadReader $reader): void
    {
        $this->columnCount = $reader->readLengthEncodedIntegerOrNull();
        $this->state = self::STATE_COLUMNS;
    }

    private function handleColumnsState(PayloadReader $reader, int $firstByte): void
    {
        if ($firstByte === self::EOF_MARKER) {
            $this->state = self::STATE_ROWS;

            return;
        }

        $this->columns[] = ColumnDefinition::fromPayload($reader);
    }

    private function handleRowsState(PayloadReader $reader): void
    {
        $reader->readFixedInteger(1);

        $nullBitmap = $this->readNullBitmap($reader);
        $row = $this->parseRow($reader, $nullBitmap);

        $this->rows[] = $row;
    }

    private function readNullBitmap(PayloadReader $reader): string
    {
        $nullBitmapBytes = (int) floor(($this->columnCount + 7) / 8);

        return $reader->readFixedString($nullBitmapBytes);
    }

    private function parseRow(PayloadReader $reader, string $nullBitmap): array
    {
        $row = [];

        foreach ($this->columns as $i => $column) {
            if ($this->isColumnNull($nullBitmap, $i)) {
                $row[$column->name] = null;

                continue;
            }

            $row[$column->name] = $this->parseColumnValue($reader, $column);
        }

        return $row;
    }

    private function isColumnNull(string $nullBitmap, int $columnIndex): bool
    {
        $byte = ord($nullBitmap[$columnIndex >> 3]);
        $bit = 1 << ($columnIndex & 7);

        return ($byte & $bit) !== 0;
    }

    private function parseColumnValue(PayloadReader $reader, ColumnDefinition $column): mixed
    {
        return match ($column->type) {
            self::FIELD_TYPE_TINY => $reader->readFixedInteger(1),
            self::FIELD_TYPE_SHORT => $reader->readFixedInteger(2),
            self::FIELD_TYPE_LONG => $reader->readFixedInteger(4),
            self::FIELD_TYPE_LONGLONG => $reader->readFixedInteger(8),
            self::FIELD_TYPE_MEDIUM => $this->parseMediumInt($reader),
            self::FIELD_TYPE_FLOAT => $this->parseFloat($reader),
            self::FIELD_TYPE_DOUBLE => $this->parseDouble($reader),
            self::FIELD_TYPE_DATE,
            self::FIELD_TYPE_TIMESTAMP,
            self::FIELD_TYPE_DATETIME => $this->parseDateTimeBinary($reader),
            self::FIELD_TYPE_TIME => $this->parseTimeBinary($reader),
            default => $reader->readLengthEncodedStringOrNull(),
        };
    }

    private function parseMediumInt(PayloadReader $reader): int
    {
        $bytes = $reader->readFixedString(3);
        $val = unpack('V', $bytes."\x00")[1];

        return ($val & 0x800000) ? ($val | ~0xFFFFFF) : $val;
    }

    private function parseFloat(PayloadReader $reader): float
    {
        return unpack('f', $reader->readFixedString(4))[1];
    }

    private function parseDouble(PayloadReader $reader): float
    {
        return unpack('d', $reader->readFixedString(8))[1];
    }

    private function parseDateTimeBinary(PayloadReader $reader): string
    {
        $length = $reader->readFixedInteger(1);

        if ($length === 0) {
            return '0000-00-00 00:00:00';
        }

        $year = $reader->readFixedInteger(2);
        $month = $reader->readFixedInteger(1);
        $day = $reader->readFixedInteger(1);

        if ($length === 4) {
            return sprintf('%04d-%02d-%02d', $year, $month, $day);
        }

        $hour = $reader->readFixedInteger(1);
        $minute = $reader->readFixedInteger(1);
        $second = $reader->readFixedInteger(1);

        if ($length === 7) {
            return sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year, $month, $day, $hour, $minute, $second);
        }

        $microsecond = $reader->readFixedInteger(4);

        return sprintf('%04d-%02d-%02d %02d:%02d:%02d.%06d', $year, $month, $day, $hour, $minute, $second, $microsecond);
    }

    private function parseTimeBinary(PayloadReader $reader): string
    {
        $length = $reader->readFixedInteger(1);

        if ($length === 0) {
            return '00:00:00';
        }

        $isNegative = $reader->readFixedInteger(1);
        $days = $reader->readFixedInteger(4);
        $hour = $reader->readFixedInteger(1);
        $minute = $reader->readFixedInteger(1);
        $second = $reader->readFixedInteger(1);

        $totalHours = ($days * 24) + $hour;
        $timeStr = sprintf('%s%02d:%02d:%02d', $isNegative ? '-' : '', $totalHours, $minute, $second);

        if ($length === 12) {
            $microsecond = $reader->readFixedInteger(4);
            $timeStr .= sprintf('.%06d', $microsecond);
        }

        return $timeStr;
    }

    public function isComplete(): bool
    {
        return $this->isComplete;
    }

    public function getResult(): Result
    {
        return new Result($this->rows);
    }

    /**
     * Reset the parser state for reuse
     */
    public function reset(): void
    {
        $this->state = self::STATE_INIT;
        $this->columnCount = null;
        $this->columns = [];
        $this->rows = [];
        $this->isComplete = false;
        $this->readerFactory = null;
    }
}
