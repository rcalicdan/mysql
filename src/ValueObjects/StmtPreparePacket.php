<?php

namespace Hibla\MysqlClient\ValueObjects;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final readonly class StmtPrepareOkPacket
{
    public function __construct(
        public int $statementId,
        public int $numColumns,
        public int $numParams,
        public int $warningCount,
    ) {}

    public static function fromPayload(PayloadReader $reader): self
    {
        $reader->readFixedInteger(1); // Skip status byte (0x00)
        $statementId = $reader->readFixedInteger(4);
        $numColumns = $reader->readFixedInteger(2);
        $numParams = $reader->readFixedInteger(2);
        $reader->readFixedInteger(1); // Skip reserved byte
        $warningCount = $reader->readFixedInteger(2);

        return new self($statementId, $numColumns, $numParams, $warningCount);
    }
}
