<?php

namespace Hibla\MysqlClient\Protocols;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class ErrPacket extends \Exception
{
    public int $errorCode;
    public string $sqlState;
    public string $errorMessage;

    public static function fromPayload(PayloadReader $reader): self
    {
        $reader->readFixedInteger(1); // Skip 0xFF header
        $errorCode = $reader->readFixedInteger(2);
        $reader->readFixedString(1); // Skip SQL state marker '#'
        $sqlState = $reader->readFixedString(5);
        $errorMessage = $reader->readRestOfPacketString();

        $packet = new self($errorMessage, $errorCode);
        $packet->errorCode = $errorCode;
        $packet->sqlState = $sqlState;
        $packet->errorMessage = $errorMessage;

        return $packet;
    }
}
