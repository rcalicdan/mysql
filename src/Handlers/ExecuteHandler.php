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
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ColumnDefinitionOrEofParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\DynamicRowOrEofParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\EofPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\MetadataOmittedRowMarker;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResponseParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResultSetHeader;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\BinaryRow;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\BinaryRowParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\TextRow;
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
     * @var Result|null The first result set in the chain, returned to the user.
     */
    private ?Result $headResult = null;

    /**
     * @var Result|null The most recent result set, used to link the next one.
     */
    private ?Result $tailResult = null;

    private ?StreamStats $primaryStreamStats = null;

    /**
     * @var DynamicRowOrEofParser|null Parser that handles dynamic Text vs Binary row detection.
     */
    private ?DynamicRowOrEofParser $rowParser = null;

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

        $this->headResult = null;
        $this->tailResult = null;
        $this->primaryStreamStats = null;
        $this->rowParser = null;

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

            if ($frame->hasMoreResults()) {
                $this->prepareDrain($result, null);
                return false;
            }

            if ($this->headResult === null) {
                $this->headResult = $result;
            } else {
                $this->linkValidResult($result);
            }

            $this->currentPromise?->resolve($this->headResult);
            return true;
        }

        if ($frame instanceof ResultSetHeader) {
            $this->state = ExecuteState::CHECK_DATA;
            return false;
        }

        throw new QueryException('Unexpected packet type in execute response header', 0);
    }

    private function prepareDrain(?Result $result, ?StreamStats $stats): void
    {
        if ($this->primaryStreamStats === null) {
            $this->primaryStreamStats = $stats;
        }

        if ($result !== null) {
            if ($this->headResult === null) {
                $this->headResult = $result;
                $this->tailResult = $result;
            } else {
                $this->linkValidResult($result);
            }
        }

        $this->state = ExecuteState::HEADER;
        $this->rows = [];
        $this->receivedNewMetadata = false;
        $this->rowParser = null;
    }

    private function linkValidResult(Result $result): void
    {
        if ($result->rowCount() > 0 || $result->getAffectedRows() > 0 || $result->hasLastInsertId() || \count($result->getFields()) > 0) {
            $this->tailResult?->setNextResult($result);
            $this->tailResult = $result;
        }
    }

    private function handleDataPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        $parser = new ColumnDefinitionOrEofParser();
        $frame = $parser->parse($reader, $length, $seq);

        if ($frame instanceof MetadataOmittedRowMarker) {
            $this->state = ExecuteState::ROWS;

            $this->rowParser = new DynamicRowOrEofParser($this->columnDefinitions, forceTextFormat: false);

            $tempParser = new BinaryRowParser($this->columnDefinitions);
            $rowFrame = $tempParser->parseRemainingRow($reader);

            $this->handleRowData($rowFrame->values);
            return false;
        }

        if ($frame instanceof EofPacket) {
            $this->state = ExecuteState::ROWS;
            $this->rowParser = new DynamicRowOrEofParser($this->columnDefinitions);
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
        if ($this->rowParser === null) {
            throw new QueryException('Row parser not initialized', 0);
        }

        $frame = $this->rowParser->parse($reader, $length, $seq);

        if ($frame instanceof EofPacket) {
            return $this->handleEndOfResultSet($frame);
        }

        if ($frame instanceof ErrPacket) {
            $exception = $this->createExceptionFromError(
                $frame->errorCode,
                "MySQL Error[{$frame->errorCode}]: {$frame->errorMessage}"
            );

            if ($this->streamContext !== null && $this->streamContext->onError !== null) {
                try {
                    ($this->streamContext->onError)($exception);
                } catch (\Throwable $e) {
                }
            }
            $this->currentPromise?->reject($exception);
            return true;
        }

        if ($frame instanceof TextRow || $frame instanceof BinaryRow) {
            $this->handleRowData($frame->values);
            return false;
        }

        throw new QueryException('Unexpected packet type in execute row data', 0);
    }

    /**
     * @param array<int, mixed> $values
     */
    private function handleRowData(array $values): void
    {
        $assocRow = [];
        /** @var array<string, int> $nameCounts */
        $nameCounts = [];

        foreach ($values as $index => $val) {
            if (! isset($this->columnDefinitions[$index])) {
                continue;
            }

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

    private function handleEndOfResultSet(EofPacket $packet): bool
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
            $currentResult = null;
            $currentStats = $stats;
        } else {
            $result = new Result(
                rows: $this->rows,
                affectedRows: 0,
                lastInsertId: 0,
                warningCount: $packet->warnings,
                columnDefinitions: $this->columnDefinitions,
                connectionId: $this->connection->getThreadId()
            );
            $currentResult = $result;
            $currentStats = null;
        }

        if ($packet->hasMoreResults()) {
            $this->prepareDrain($currentResult, $currentStats);
            return false;
        }

        if ($this->streamContext !== null) {
            $finalStats = $this->primaryStreamStats ?? $currentStats;
            if ($this->streamContext->onComplete !== null && $finalStats !== null) {
                try {
                    ($this->streamContext->onComplete)($finalStats);
                } catch (\Throwable $e) {
                }
            }
            if ($finalStats !== null) {
                $this->currentPromise?->resolve($finalStats);
            }
        } else {
            if ($currentResult !== null) {
                if ($this->headResult === null) {
                    $this->headResult = $currentResult;
                } else {
                    $this->linkValidResult($currentResult);
                }
            }

            if ($this->headResult !== null) {
                $this->currentPromise?->resolve($this->headResult);
            }
        }

        return true;
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
            return new ConstraintViolationException($message . ' - ' . $constraintErrors[$errorCode], $errorCode);
        }

        return new QueryException($message, $errorCode);
    }
}