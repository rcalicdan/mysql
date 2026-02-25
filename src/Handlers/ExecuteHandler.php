<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Enums\ExecuteState;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Internals\Result;
use Hibla\Mysql\ValueObjects\StreamContext;
use Hibla\Mysql\ValueObjects\StreamStats;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConstraintViolationException;
use Hibla\Sql\Exceptions\QueryException;
use Rcalicdan\MySQLBinaryProtocol\Exception\IncompleteBufferException;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\BinaryRowOrEofParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ColumnDefinitionOrEofParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\EofPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\MetadataOmittedRowMarker;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResponseParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResultSetHeader;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\BinaryRow;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Handles binary protocol prepared statement execution (COM_STMT_EXECUTE).
 *
 * @internal
 */
final class ExecuteHandler
{
    private int $sequenceId = 0;

    private int $streamedRowCount = 0;

    private float $streamStartTime = 0.0;

    private bool $receivedNewMetadata = false;

    private ExecuteState $state = ExecuteState::HEADER;

    private ?StreamContext $streamContext = null;

    /**
     *  @var Promise<Result|StreamStats>|null
     */
    private ?Promise $currentPromise = null;

    /**
     *  @var array<int, array<string, mixed>>
     */
    private array $rows = [];

    /**
     *  @var array<int, ColumnDefinition>
     */
    private array $columnDefinitions = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly CommandBuilder $commandBuilder
    ) {
    }

    /**
     * @param array<int, mixed> $params
     * @param array<int, ColumnDefinition> $columnDefinitions
     * @param Promise<Result|StreamStats> $promise
     */
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
        $this->streamStartTime = (float) hrtime(true);

        $packet = $this->commandBuilder->buildStmtExecute($stmtId, array_values($params));

        $this->connection->writePacket($packet, $this->sequenceId);
    }

    public function processPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        try {
            $this->sequenceId = $seq + 1;

            return match ($this->state) {
                ExecuteState::HEADER => $this->handleHeader($reader, $length, $seq),
                ExecuteState::CHECK_DATA => $this->handleDataPacket($reader, $length, $seq),
                ExecuteState::ROWS => $this->handleRow($reader, $length, $seq),
            };
        } catch (IncompleteBufferException $e) {
            throw $e;
        } catch (\Throwable $e) {
            if (! $e instanceof QueryException) {
                $e = new QueryException(
                    'Failed to execute prepared statement: ' . $e->getMessage(),
                    (int)$e->getCode(),
                    $e
                );
            }

            if ($this->streamContext !== null && $this->streamContext->onError !== null) {
                try {
                    ($this->streamContext->onError)($e);
                } catch (\Throwable $callbackError) {
                    // Ignore errors in error handler
                }
            }

            $this->currentPromise?->reject($e);

            try {
                $reader->readRestOfPacketString();
            } catch (\Throwable $t) {
                // Ignore cleanup errors
            }

            return true;
        }
    }

    private function handleHeader(PayloadReader $reader, int $length, int $seq): bool
    {
        $responseParser = new ResponseParser();
        $frame = $responseParser->parseResponse($reader, $length, $seq);

        if ($frame instanceof ErrPacket) {
            $this->currentPromise?->reject(
                $this->createExceptionFromError($frame->errorCode, $frame->errorMessage)
            );

            return true;
        }

        if ($frame instanceof OkPacket) {
            $result = new Result(
                rows: [],
                affectedRows: $frame->affectedRows,
                lastInsertId: $frame->lastInsertId,
                warningCount: $frame->warnings,
                columnDefinitions: [],
                connectionId: $this->connection->getThreadId()
            );
            $this->currentPromise?->resolve($result);

            return true;
        }

        if ($frame instanceof ResultSetHeader) {
            $this->state = ExecuteState::CHECK_DATA;

            return false;
        }

        throw new QueryException('Unexpected packet type in execute response header', 0);
    }

    private function handleDataPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        $parser = new ColumnDefinitionOrEofParser();
        $frame = $parser->parse($reader, $length, $seq);

        // Optimization: Server omitted metadata, the 0x00 byte was actually the start of a BinaryRow
        if ($frame instanceof MetadataOmittedRowMarker) {
            $this->state = ExecuteState::ROWS;
            $rowParser = new BinaryRowOrEofParser($this->columnDefinitions);
            $this->handleBinaryRow($rowParser->parseRemainingRow($reader));

            return false;
        }

        if ($frame instanceof EofPacket) {
            $this->state = ExecuteState::ROWS;

            return false;
        }

        if ($frame instanceof ColumnDefinition) {
            if (! $this->receivedNewMetadata) {
                $this->columnDefinitions = [];
                $this->receivedNewMetadata = true;
            }

            $this->columnDefinitions[] = $frame;

            return false;
        }

        throw new QueryException('Unexpected packet type in execute column definitions', 0);
    }

    private function handleRow(PayloadReader $reader, int $length, int $seq): bool
    {
        $parser = new BinaryRowOrEofParser($this->columnDefinitions);
        $frame = $parser->parse($reader, $length, $seq);

        if ($frame instanceof EofPacket) {
            $this->handleEndOfResultSet($frame);

            return true;
        }

        if ($frame instanceof ErrPacket) {
            $exception = $this->createExceptionFromError(
                $frame->errorCode,
                "MySQL Error [{$frame->errorCode}]: {$frame->errorMessage}"
            );

            if ($this->streamContext !== null && $this->streamContext->onError !== null) {
                try {
                    ($this->streamContext->onError)($exception);
                } catch (\Throwable $e) {
                    // Ignore error handler errors
                }
            }

            $this->currentPromise?->reject($exception);

            return true;
        }

        if ($frame instanceof BinaryRow) {
            $this->handleBinaryRow($frame);

            return false;
        }

        throw new QueryException('Unexpected packet type in execute rows', 0);
    }

    private function handleBinaryRow(BinaryRow $row): void
    {
        $assocRow = [];
        /** @var array<string, int> $nameCounts */
        $nameCounts = [];

        foreach ($row->values as $index => $val) {
            $colName = $this->columnDefinitions[$index]->name;

            if (isset($nameCounts[$colName])) {
                $suffix = $nameCounts[$colName]++;
                $colName .= (string) $suffix;
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
    }

    private function handleEndOfResultSet(EofPacket $packet): void
    {
        if ($this->streamContext !== null) {
            $duration = ((float)hrtime(true) - $this->streamStartTime) / 1e9;

            $stats = new StreamStats(
                rowCount: $this->streamedRowCount,
                columnCount: \count($this->columnDefinitions),
                duration: $duration,
                warningCount: $packet->warnings,
                connectionId: $this->connection->getThreadId()
            );

            if ($this->streamContext->onComplete !== null) {
                try {
                    ($this->streamContext->onComplete)($stats);
                } catch (\Throwable $e) {
                    // Ignore completion handler errors
                }
            }

            $this->currentPromise?->resolve($stats);
        } else {
            $result = new Result(
                rows: $this->rows,
                affectedRows: 0,
                lastInsertId: 0,
                warningCount: $packet->warnings,
                columnDefinitions: $this->columnDefinitions,
                connectionId: $this->connection->getThreadId()
            );
            $this->currentPromise?->resolve($result);
        }
    }

    private function createExceptionFromError(int $errorCode, string $message): \Throwable
    {
        $constraintErrors = [
            1062 => 'Duplicate entry (UNIQUE constraint)',
            1451 => 'Cannot delete parent row (FOREIGN KEY constraint)',
            1452 => 'Cannot add child row (FOREIGN KEY constraint)',
            1048 => 'Column cannot be null (NOT NULL constraint)',
            3819 => 'Check constraint violated',
            1216 => 'Cannot add foreign key constraint',
            1217 => 'Cannot delete foreign key constraint',
            1364 => 'Field doesn\'t have default value (NOT NULL constraint)',
        ];

        if (isset($constraintErrors[$errorCode])) {
            return new ConstraintViolationException(
                $message . ' - ' . $constraintErrors[$errorCode],
                $errorCode
            );
        }

        return new QueryException($message, $errorCode);
    }
}
