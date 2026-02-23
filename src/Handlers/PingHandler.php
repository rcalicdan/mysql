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

final class PingHandler
{
    private int $sequenceId = 0;

    /**
     *  @var Promise<bool>|null
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

        $payload = \chr(0x0E); // Command: PING

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
                $exception = $this->createPingException($frame);
                $this->currentPromise?->reject($exception);

                return true;
            }

            $exception = new ConnectionException(
                'Unexpected packet type in ping response: expected OK or ERR packet',
                0
            );
            $this->currentPromise?->reject($exception);

            return true;
        } catch (\Throwable $e) {
            if (! $e instanceof ConnectionException) {
                $e = new ConnectionException(
                    'Failed to process ping response: ' . $e->getMessage(),
                    (int)$e->getCode(),
                    $e
                );
            }

            $this->currentPromise?->reject($e);

            return true;
        }
    }

    private function createPingException(ErrPacket $packet): ConnectionException
    {
        $pingErrorCodes = [
            2006 => 'Server has gone away - Connection lost',
            2013 => 'Lost connection to MySQL server during query',
            1053 => 'Server shutdown in progress',
            1317 => 'Query execution was interrupted',
            2003 => 'Cannot connect to MySQL server',
            2002 => 'Cannot connect to local MySQL server through socket',
        ];

        $errorDescription = $pingErrorCodes[$packet->errorCode] ?? 'Ping failed';

        return new ConnectionException(
            "MySQL Ping Error [{$packet->errorCode}]: {$packet->errorMessage} - {$errorDescription}",
            $packet->errorCode
        );
    }
}
