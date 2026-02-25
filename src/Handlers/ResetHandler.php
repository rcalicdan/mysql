<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Mysql\Internals\Connection;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResponseParser;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

/**
 * Handles the COM_RESET_CONNECTION (0x1F) command.
 *
 * @internal
 */
final class ResetHandler
{
    private int $sequenceId = 0;

    /**
     * @var Promise<mixed>|null
     */
    private ?Promise $currentPromise = null;

    public function __construct(
        private readonly Connection $connection
    ) {
    }

    /**
     * @param Promise<bool> $promise
     */
    public function start(Promise $promise): void
    {
        $this->currentPromise = $promise;
        $this->sequenceId = 0;

        $payload = \chr(0x1F);

        $this->connection->writePacket($payload, $this->sequenceId);
    }

    public function processPacket(PayloadReader $reader, int $length, int $seq): bool
    {
        try {
            $responseParser = new ResponseParser();
            $frame = $responseParser->parseResponse($reader, $length, $seq);

            if ($frame instanceof OkPacket) {
                $this->currentPromise?->resolve(true);

                return true;
            }

            if ($frame instanceof ErrPacket) {
                $exception = new ConnectionException(
                    "MySQL Reset Connection Error [{$frame->errorCode}]: {$frame->errorMessage}",
                    $frame->errorCode
                );
                $this->currentPromise?->reject($exception);

                return true;
            }

            $exception = new ConnectionException('Unexpected packet type in reset response', 0);
            $this->currentPromise?->reject($exception);

            return true;

        } catch (\Throwable $e) {
            if (! $e instanceof ConnectionException) {
                $e = new ConnectionException('Failed to process reset response: ' . $e->getMessage(), (int)$e->getCode(), $e);
            }
            $this->currentPromise?->reject($e);

            return true;
        }
    }
}
