<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Enums\PrepareState;
use Hibla\Mysql\Internals\Connection;
use Hibla\Mysql\Internals\PreparedStatement;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\PreparedException;
use Rcalicdan\MySQLBinaryProtocol\Frame\Command\CommandBuilder;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ColumnDefinitionOrEofParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\EofPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\StmtPrepareOkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\StmtPrepareResponseParser;
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
        private readonly Connection $connection,
        private readonly CommandBuilder $commandBuilder
    ) {
    }

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

        $this->connection->writePacket($packet, $this->sequenceId);
    }

    public function processPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        try {
            $this->sequenceId = $seq + 1;

            return match ($this->state) {
                PrepareState::HEADER => $this->handleHeader($reader, $length, $seq),
                PrepareState::DRAIN_PARAMS => $this->drainDefinitions($reader, $length, $seq, 'params'),
                PrepareState::DRAIN_COLUMNS => $this->drainDefinitions($reader, $length, $seq, 'columns'),
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

    private function handleHeader(PayloadReader $reader, int $length, int $seq): bool
    {
        $parser = new StmtPrepareResponseParser();
        $frame = $parser->parse($reader, $length, $seq);

        if ($frame instanceof ErrPacket) {
            $exception = new PreparedException(
                "Failed to prepare statement [Error {$frame->errorCode}] [{$frame->sqlState}]: {$frame->errorMessage}",
                $frame->errorCode
            );

            $this->currentPromise?->reject($exception);

            return true;
        }

        if ($frame instanceof StmtPrepareOkPacket) {
            $this->stmtId = $frame->statementId;
            $this->numColumns = $frame->numColumns;
            $this->numParams = $frame->numParams;

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

        throw new PreparedException('Unexpected packet type in prepare response header', 0);
    }

    private function drainDefinitions(PayloadReader $reader, int $length, int $seq, string $type): bool
    {
        $parser = new ColumnDefinitionOrEofParser();
        $frame = $parser->parse($reader, $length, $seq);

        if ($frame instanceof EofPacket) {
            if ($type === 'params' && $this->numColumns > 0) {
                $this->state = PrepareState::DRAIN_COLUMNS;

                return false;
            }

            $this->finish();

            return true;
        }

        if ($frame instanceof ColumnDefinition) {
            if ($type === 'columns') {
                $this->columnDefinitions[] = $frame;
            } else {
                $this->paramDefinitions[] = $frame;
            }

            return false;
        }

        if ($frame instanceof ErrPacket) {
            $exception = new PreparedException(
                "Failed to parse {$type} definition [Error {$frame->errorCode}] [{$frame->sqlState}]: {$frame->errorMessage}",
                $frame->errorCode
            );
            $this->currentPromise?->reject($exception);

            return true;
        }

        throw new PreparedException("Unexpected packet type in prepare {$type} definitions", 0);
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
}
