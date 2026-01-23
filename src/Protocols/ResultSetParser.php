<?php

namespace Hibla\MysqlClient\Protocols;

use Hibla\MysqlClient\Enums\PacketMarker;
use Hibla\MysqlClient\Enums\ParserState;
use Hibla\MysqlClient\ValueObjects\ColumnDefinition;
use Hibla\MysqlClient\ValueObjects\Result;
use Rcalicdan\MySQLBinaryProtocol\Buffer\Reader\BufferPayloadReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Parses a result set that is returned in the Text Protocol format.
 * This is used for regular query results (non-prepared statements).
 */
final class ResultSetParser
{
    private const int EOF_PACKET_MAX_LENGTH = 9;

    private array $columns = [];

    private array $rows = [];

    private bool $isComplete = false;

    private ?int $columnCount = null;

    private ?Result $finalResult = null;

    private ?BufferPayloadReaderFactory $readerFactory = null;

    private ParserState $state = ParserState::INIT;

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
            ParserState::COLUMNS => $this->handleColumnsState($reader, $firstByte, $rawPayload),
            ParserState::ROWS => $this->handleRowsState($reader),
        };
    }

    public function isComplete(): bool
    {
        return $this->isComplete;
    }

    public function getResult(): Result
    {
        return $this->finalResult ?? new Result([]);
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
        $this->finalResult = null;
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
            && $this->isEofPacket($firstByte, $rawPayload);
    }

    private function isEofPacket(int $firstByte, string $rawPayload): bool
    {
        return $firstByte === PacketMarker::EOF->value
            && \strlen($rawPayload) < self::EOF_PACKET_MAX_LENGTH;
    }

    private function markComplete(): void
    {
        $this->finalResult = new Result($this->rows);
        $this->isComplete = true;
        $this->columns = [];
        $this->readerFactory = null;
    }

    private function handleInitState(PayloadReader $reader): void
    {
        $this->columnCount = $reader->readLengthEncodedIntegerOrNull();
        $this->state = ParserState::COLUMNS;
    }

    private function handleColumnsState(
        PayloadReader $reader,
        int $firstByte,
        string $rawPayload
    ): void {
        if ($this->isEofPacket($firstByte, $rawPayload)) {
            $this->state = ParserState::ROWS;

            return;
        }

        $this->columns[] = ColumnDefinition::fromPayload($reader);
    }

    private function handleRowsState(PayloadReader $reader): void
    {
        $row = $this->parseRow($reader);
        $this->rows[] = $row;
    }

    private function parseRow(PayloadReader $reader): array
    {
        $row = [];

        foreach ($this->columns as $column) {
            $row[$column->name] = $reader->readLengthEncodedStringOrNull();
        }

        return $row;
    }
}
