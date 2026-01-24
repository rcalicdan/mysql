<?php

namespace Hibla\MysqlClient\ValueObjects;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class ErrPacket extends \Exception
{
    public function __construct(
        public int $errorCode,
        public string $sqlState,
        public string $errorMessage,
    ) {
        parent::__construct($errorMessage, $errorCode);
    }

    public static function fromPayload(PayloadReader $reader): self
    {
        $reader->readFixedInteger(1); // Skip 0xFF header
        $errorCode = $reader->readFixedInteger(2);
        $reader->readFixedString(1); // Skip SQL state marker '#'
        $sqlState = $reader->readFixedString(5);
        $errorMessage = $reader->readRestOfPacketString();

        return new self($errorCode, $sqlState, $errorMessage);
    }
}