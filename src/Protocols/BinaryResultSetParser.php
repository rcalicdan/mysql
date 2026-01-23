<?php

namespace Hibla\MysqlClient\Protocols;

use Hibla\MysqlClient\Enums\FieldType;
use Hibla\MysqlClient\Enums\PacketMarker;
use Hibla\MysqlClient\Enums\ParserState;
use Hibla\MysqlClient\ValueObjects\ColumnDefinition;
use Hibla\MysqlClient\ValueObjects\Result;
use Rcalicdan\MySQLBinaryProtocol\Buffer\Reader\BufferPayloadReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Parses a result set that is returned in the Binary Row Protocol format.
 * This is used for the results of prepared statements.
 */
final class BinaryResultSetParser
{
    private const int EOF_PACKET_MAX_LENGTH = 9;

    private array $columns = [];

    private array $rows = [];

    private bool $isComplete = false;

    private ?int $columnCount = null;

    private ParserState $state = ParserState::INIT;

    private ?BufferPayloadReaderFactory $readerFactory = null;

    public function processPayload(string $rawPayload): void
    {
        if ($this->isComplete) {
            return;
        }

        $reader = $this->getReader($rawPayload);
        $firstByte = \ord($rawPayload[0]);

        if ($this->isEndOfResultSet($firstByte, $rawPayload)) {
            $this->markComplete();

            return;
        }

        match ($this->state) {
            ParserState::INIT => $this->handleInitState($reader),
            ParserState::COLUMNS => $this->handleColumnsState($reader, $firstByte),
            ParserState::ROWS => $this->handleRowsState($reader),
        };
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
        $this->state = ParserState::INIT;
        $this->columnCount = null;
        $this->columns = [];
        $this->rows = [];
        $this->isComplete = false;
        $this->readerFactory = null;
    }

    private function getReader(string $rawPayload): PayloadReader
    {
        $this->readerFactory ??= new BufferPayloadReaderFactory;

        return $this->readerFactory->createFromString($rawPayload);
    }

    private function isEndOfResultSet(int $firstByte, string $rawPayload): bool
    {
        return $this->state === ParserState::ROWS
            && $firstByte === PacketMarker::EOF->value
            && \strlen($rawPayload) < self::EOF_PACKET_MAX_LENGTH;
    }

    private function markComplete(): void
    {
        $this->isComplete = true;
        $this->columns = [];
        $this->readerFactory = null;
    }

    private function handleInitState(PayloadReader $reader): void
    {
        $this->columnCount = $reader->readLengthEncodedIntegerOrNull();
        $this->state = ParserState::COLUMNS;
    }

    private function handleColumnsState(PayloadReader $reader, int $firstByte): void
    {
        if ($firstByte === PacketMarker::EOF->value) {
            $this->state = ParserState::ROWS;

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
        $byte = \ord($nullBitmap[$columnIndex >> 3]);
        $bit = 1 << ($columnIndex & 7);

        return ($byte & $bit) !== 0;
    }

    private function parseColumnValue(PayloadReader $reader, ColumnDefinition $column): mixed
    {
        return match ($column->type) {
            FieldType::TINY->value => $reader->readFixedInteger(1),
            FieldType::SHORT->value => $reader->readFixedInteger(2),
            FieldType::LONG->value => $reader->readFixedInteger(4),
            FieldType::LONGLONG->value => $reader->readFixedInteger(8),
            FieldType::MEDIUM->value => $this->parseMediumInt($reader),
            FieldType::FLOAT->value => $this->parseFloat($reader),
            FieldType::DOUBLE->value => $this->parseDouble($reader),
            FieldType::DATE->value,
            FieldType::TIMESTAMP->value,
            FieldType::DATETIME->value => $this->parseDateTimeBinary($reader),
            FieldType::TIME->value => $this->parseTimeBinary($reader),
            FieldType::YEAR->value => $reader->readFixedInteger(4),
            FieldType::DECIMAL->value,
            FieldType::NEWDECIMAL->value => $reader->readLengthEncodedStringOrNull(),
            FieldType::ENUM->value,
            FieldType::SET->value => $reader->readLengthEncodedStringOrNull(),
            FieldType::JSON->value => $this->parseJson($reader),
            FieldType::TINY_BLOB->value,
            FieldType::BLOB->value,
            FieldType::MEDIUM_BLOB->value,
            FieldType::LONG_BLOB->value => $reader->readLengthEncodedStringOrNull(),
            FieldType::STRING->value,
            FieldType::VAR_STRING->value => $reader->readLengthEncodedStringOrNull(),
            default => $reader->readLengthEncodedStringOrNull(),
        };
    }

    private function parseJson(PayloadReader $reader): ?string
    {
        return $reader->readLengthEncodedStringOrNull();
    }

    private function parseMediumInt(PayloadReader $reader): int
    {
        $bytes = $reader->readFixedString(3);
        $val = unpack('V', $bytes . "\x00")[1];

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
            return \sprintf('%04d-%02d-%02d', $year, $month, $day);
        }

        $hour = $reader->readFixedInteger(1);
        $minute = $reader->readFixedInteger(1);
        $second = $reader->readFixedInteger(1);

        if ($length === 7) {
            return \sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year, $month, $day, $hour, $minute, $second);
        }

        $microsecond = $reader->readFixedInteger(4);

        return \sprintf('%04d-%02d-%02d %02d:%02d:%02d.%06d', $year, $month, $day, $hour, $minute, $second, $microsecond);
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
        $timeStr = \sprintf('%s%02d:%02d:%02d', $isNegative ? '-' : '', $totalHours, $minute, $second);

        if ($length === 12) {
            $microsecond = $reader->readFixedInteger(4);
            $timeStr .= \sprintf('.%06d', $microsecond);
        }

        return $timeStr;
    }
}
