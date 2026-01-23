<?php

namespace Hibla\MysqlClient\Protocols;

use Hibla\MysqlClient\ValueObjects\ColumnDefinition;
use Hibla\MysqlClient\ValueObjects\Result;
use Rcalicdan\MySQLBinaryProtocol\Buffer\Reader\BufferPayloadReaderFactory;

final class ResultSetParser
{
    private const STATE_INIT = 0;
    private const STATE_COLUMNS = 1;
    private const STATE_ROWS = 2;

    private const EOF_PACKET_MARKER = 0xFE;
    private const EOF_PACKET_MAX_LENGTH = 9;

    private int $state = self::STATE_INIT;
    private ?int $columnCount = null;
    private array $columns = [];
    private array $rows = [];
    private bool $isComplete = false;
    private mixed $finalResult = null;
    private BufferPayloadReaderFactory $readerFactory;

    public function __construct()
    {
        $this->readerFactory = new BufferPayloadReaderFactory;
    }

    public function processPayload(string $rawPayload): void
    {
        if ($this->isComplete) {
            return;
        }

        $reader = $this->readerFactory->createFromString($rawPayload);
        $firstByte = \ord($rawPayload[0]);

        if ($this->isEofPacketDuringRows($firstByte, $rawPayload)) {
            $this->completeResultSet();

            return;
        }

        $this->processPayloadByState($reader, $firstByte, $rawPayload);
    }

    public function isComplete(): bool
    {
        return $this->isComplete;
    }

    public function getResult(): mixed
    {
        return $this->finalResult;
    }

    private function isEofPacketDuringRows(int $firstByte, string $rawPayload): bool
    {
        return $this->state === self::STATE_ROWS &&
            $this->isEofPacket($firstByte, $rawPayload);
    }

    private function isEofPacket(int $firstByte, string $rawPayload): bool
    {
        return $firstByte === self::EOF_PACKET_MARKER &&
            \strlen($rawPayload) < self::EOF_PACKET_MAX_LENGTH;
    }

    private function completeResultSet(): void
    {
        $this->finalResult = new Result($this->rows);
        $this->isComplete = true;
    }

    private function processPayloadByState($reader, int $firstByte, string $rawPayload): void
    {
        switch ($this->state) {
            case self::STATE_INIT:
                $this->processInitialPayload($reader);

                break;

            case self::STATE_COLUMNS:
                $this->processColumnPayload($reader, $firstByte, $rawPayload);

                break;

            case self::STATE_ROWS:
                $this->processRowPayload($reader);

                break;
        }
    }

    private function processInitialPayload($reader): void
    {
        $this->columnCount = $reader->readLengthEncodedIntegerOrNull();
        $this->state = self::STATE_COLUMNS;
    }

    private function processColumnPayload($reader, int $firstByte, string $rawPayload): void
    {
        if ($this->isEofPacket($firstByte, $rawPayload)) {
            $this->transitionToRowsState();

            return;
        }

        $this->addColumnDefinition($reader);
    }

    private function transitionToRowsState(): void
    {
        $this->state = self::STATE_ROWS;
    }

    private function addColumnDefinition($reader): void
    {
        $this->columns[] = ColumnDefinition::fromPayload($reader);
    }

    private function processRowPayload($reader): void
    {
        $row = $this->buildRowFromColumns($reader);
        $this->rows[] = $row;
    }

    private function buildRowFromColumns($reader): array
    {
        $row = [];

        foreach ($this->columns as $column) {
            $row[$column->name] = $reader->readLengthEncodedStringOrNull();
        }

        return $row;
    }
}
