<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ErrPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\OkPacket;
use Rcalicdan\MySQLBinaryProtocol\Frame\Response\ResponseParser;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class PingHandler
{
    private int $sequenceId = 0;
    private ?Promise $currentPromise = null;

    public function __construct(
        private readonly SocketConnection $socket
    ) {}

    public function start(Promise $promise): void
    {
        $this->currentPromise = $promise;
        $this->sequenceId = 0;

        $payload = \chr(0x0E);
        $this->writePacket($payload);
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
                $this->currentPromise?->reject(
                    new \RuntimeException("Ping failed: {$frame->errorMessage}")
                );

                return true;
            }

            throw new \RuntimeException('Unexpected packet type in ping response');
        } catch (\Throwable $e) {
            $this->currentPromise?->reject($e);

            return true;
        }
    }

    private function writePacket(string $payload): void
    {
        $len = \strlen($payload);
        $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
        $this->socket->write($header . $payload);
        $this->sequenceId++;
    }
}
