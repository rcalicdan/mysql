<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Enums\ParserState;
use Hibla\MysqlClient\ValueObjects\Result;
use Rcalicdan\MySQLBinaryProtocol\Frame\Error\ErrPacketParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacketParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinitionParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\TextRowParser;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Handles parsing of MySQL query responses (OK, Error, or Result Set)
 */
class ResponseHandler
{
    private ParserState $state = ParserState::INIT;
    
    private bool $complete = false;
    
    private bool $hasError = false;
    
    private ?\Throwable $error = null;
    
    private mixed $result = null;
    
    private int $columnCount = 0;
    
    /**
     * @var array<int, \Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition>
     */
    private array $columns = [];
    
    /**
     * @var array<int, array<string, mixed>>
     */
    private array $rows = [];

    public function processPacket(PayloadReader $payload, int $length, int $sequence): void
    {
        if ($this->complete) {
            return;
        }

        try {
            if ($this->state === ParserState::INIT) {
                $this->processFirstPacket($payload, $length, $sequence);
            } elseif ($this->state === ParserState::COLUMNS) {
                $this->processColumnPacket($payload, $length, $sequence);
            } elseif ($this->state === ParserState::ROWS) {
                $this->processRowPacket($payload, $length, $sequence);
            }
        } catch (\Throwable $e) {
            $this->hasError = true;
            $this->error = $e;
            $this->complete = true;
        }
    }

    public function isComplete(): bool
    {
        return $this->complete;
    }

    public function hasError(): bool
    {
        return $this->hasError;
    }

    public function getError(): ?\Throwable
    {
        return $this->error;
    }

    public function getResult(): mixed
    {
        return $this->result;
    }

    private function processFirstPacket(PayloadReader $payload, int $length, int $sequence): void
    {
        $firstByte = $payload->readFixedInteger(1);

        if ($firstByte === 0x00) {
            // OK Packet
            $parser = new OkPacketParser();
            $ok = $parser->parse($payload, $length, $sequence);
            
            $this->result = $ok;
            $this->complete = true;
        } elseif ($firstByte === 0xFF) {
            // Error Packet
            $parser = new ErrPacketParser();
            $err = $parser->parse($payload, $length, $sequence);
            
            $this->hasError = true;
            $this->error = new \RuntimeException(
                "{$err->errorMessage} (Code: {$err->errorCode}, SQL State: {$err->sqlState})"
            );
            $this->complete = true;
        } else {
            // Result Set - first byte is column count (length-encoded integer)
            // We already read the first byte, so we need to handle it properly
            if ($firstByte < 251) {
                $this->columnCount = (int) $firstByte;
            } else {
                // This is a larger length-encoded integer
                // For simplicity, we'll treat it as the column count directly
                $this->columnCount = (int) $firstByte;
            }
            
            $this->state = ParserState::COLUMNS;
        }
    }

    private function processColumnPacket(PayloadReader $payload, int $length, int $sequence): void
    {
        $parser = new ColumnDefinitionParser();
        $column = $parser->parse($payload, $length, $sequence);
        
        $this->columns[] = $column;

        // Check if we've received all columns
        if (count($this->columns) === $this->columnCount) {
            $this->state = ParserState::ROWS;
        }
    }

    private function processRowPacket(PayloadReader $payload, int $length, int $sequence): void
    {
        $firstByte = $payload->readFixedInteger(1);

        // EOF packet (0xFE) with length < 9 indicates end of rows
        if ($firstByte === 0xFE && $length < 9) {
            // End of result set
            $this->result = new Result($this->rows);
            $this->complete = true;
            return;
        }

        // It's a row packet - parse it
        $parser = new TextRowParser($this->columnCount);
        $row = $parser->parse($payload, $length, $sequence);

        // Convert to associative array
        $assocRow = [];
        foreach ($row->values as $i => $value) {
            $columnName = $this->columns[$i]->name;
            $assocRow[$columnName] = $value;
        }

        $this->rows[] = $assocRow;
    }
}