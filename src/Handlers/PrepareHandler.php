<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Enums\PrepareState;
use Hibla\Mysql\Internals\Connection as MysqlConnection;
use Hibla\Mysql\Internals\PreparedStatement;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Hibla\Sql\Exceptions\PreparedException;
use Rcalicdan\MySQLBinaryProtocol\Constants\PacketType;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class PrepareHandler
{
    /**
     *  @var array<int, ColumnDefinition> 
     */
    private array $columnDefinitions = [];

    /**
     *  @var array<int, ColumnDefinition> 
     */
    private array $paramDefinitions = [];

    /**
     *  @var Promise<PreparedStatement>|null 
     */
    private ?Promise $currentPromise = null;

    private PrepareState $state = PrepareState::HEADER;

    private int $sequenceId = 0;

    private int $stmtId = 0;

    private int $numColumns = 0;

    private int $numParams = 0;

    public function __construct(
        private readonly MysqlConnection $connection,
        private readonly SocketConnection $socket,
        private readonly CommandBuilder $commandBuilder
    ) {}

    /**
     * @param Promise<PreparedStatement> $promise
     */
    public function start(string $sql, Promise $promise): void
    {
        $this->state = PrepareState::HEADER;
        $this->currentPromise = $promise;
        $this->sequenceId = 0;
        $this->columnDefinitions = [];
        $this->paramDefinitions = [];

        $packet = $this->commandBuilder->buildStmtPrepare($sql);
        $this->writePacket($packet);
    }

    public function processPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        try {
            $this->sequenceId = $seq + 1;

            return match ($this->state) {
                PrepareState::HEADER => $this->handleHeader($reader, $length),
                PrepareState::DRAIN_PARAMS => $this->drainDefinitions($reader, $length, 'params'),
                PrepareState::DRAIN_COLUMNS => $this->drainDefinitions($reader, $length, 'columns'),
            };
        } catch (\Throwable $e) {
            if (! $e instanceof PreparedException) {
                $e = new PreparedException(
                    'Failed to prepare statement: ' . $e->getMessage(),
                    (int)$e->getCode(),
                    $e
                );
            }

            $this->currentPromise?->reject($e);

            return true;
        }
    }

    private function handleHeader(PayloadReader $reader, int $length): bool
    {
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === PacketType::ERR) {
            $code = (int)$reader->readFixedInteger(2);
            $reader->readFixedString(1);
            $sqlState = $reader->readFixedString(5);
            $msg = $reader->readRestOfPacketString();

            $exception = new PreparedException(
                "Failed to prepare statement [Error {$code}] [{$sqlState}]: {$msg}",
                $code
            );

            $this->currentPromise?->reject($exception);

            return true;
        }

        if ($firstByte === PacketType::OK) {
            $this->stmtId = (int)$reader->readFixedInteger(4);
            $this->numColumns = (int)$reader->readFixedInteger(2);
            $this->numParams = (int)$reader->readFixedInteger(2);
            $reader->readFixedInteger(1);
            $reader->readFixedInteger(2);

            if ($this->numParams > 0) {
                $this->state = PrepareState::DRAIN_PARAMS;

                return false;
            }
            if ($this->numColumns > 0) {
                $this->state = PrepareState::DRAIN_COLUMNS;

                return false;
            }

            $this->finish();

            return true;
        }

        throw new PreparedException(
            'Unexpected packet type in prepare response header: 0x' . dechex((int)$firstByte),
            0
        );
    }

    private function drainDefinitions(PayloadReader $reader, int $length, string $type): bool
    {
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === PacketType::EOF && $length < PacketType::EOF_MAX_LENGTH) {
            if ($length > 1) {
                $reader->readFixedString($length - 1);
            }

            if ($type === 'params' && $this->numColumns > 0) {
                $this->state = PrepareState::DRAIN_COLUMNS;

                return false;
            }

            $this->finish();

            return true;
        }

        try {
            $reader->readFixedString((int)$firstByte);

            $schema = $reader->readLengthEncodedStringOrNull() ?? '';
            $table = $reader->readLengthEncodedStringOrNull() ?? '';
            $orgTable = $reader->readLengthEncodedStringOrNull() ?? '';
            $name = $reader->readLengthEncodedStringOrNull() ?? '';
            $orgName = $reader->readLengthEncodedStringOrNull() ?? '';
            $reader->readLengthEncodedIntegerOrNull();

            $charset = (int)$reader->readFixedInteger(2);
            $colLength = (int)$reader->readFixedInteger(4);
            $typeCode = (int)$reader->readFixedInteger(1);
            $flags = (int)$reader->readFixedInteger(2);
            $decimals = (int)$reader->readFixedInteger(1);
            $reader->readRestOfPacketString();

            $def = new ColumnDefinition(
                '',
                $schema,
                $table,
                $orgTable,
                $name,
                $orgName,
                $charset,
                $colLength,
                $typeCode,
                $flags,
                $decimals
            );

            if ($type === 'columns') {
                $this->columnDefinitions[] = $def;
            } else {
                $this->paramDefinitions[] = $def;
            }

            return false;
        } catch (\Throwable $e) {
            throw new PreparedException(
                "Failed to parse {$type} definition: " . $e->getMessage(),
                0,
                $e
            );
        }
    }

    private function finish(): void
    {
        $stmt = new PreparedStatement(
            $this->connection,
            $this->stmtId,
            $this->numColumns,
            $this->numParams,
            $this->columnDefinitions,
            $this->paramDefinitions
        );

        $this->currentPromise?->resolve($stmt);
    }

    private function writePacket(string $payload): void
    {
        $len = \strlen($payload);
        $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
        $this->socket->write($header . $payload);
        $this->sequenceId++;
    }
}
