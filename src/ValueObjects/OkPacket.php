<?php

namespace Hibla\MysqlClient\ValueObjects;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final readonly class OkPacket
{
    public function __construct(
        public int $affectedRows,
        public int $lastInsertId,
        public int $statusFlags,
        public int $warnings,
        public string $info = '',
    ) {}

    public static function fromPayload(PayloadReader $reader): self
    {
        $reader->readFixedInteger(1); // Skip 0x00 header
        $affectedRows = $reader->readLengthEncodedIntegerOrNull() ?? 0;
        $lastInsertId = $reader->readLengthEncodedIntegerOrNull() ?? 0;
        $statusFlags = $reader->readFixedInteger(2);
        $warnings = $reader->readFixedInteger(2);

        // Read info string if available
        $info = '';
        try {
            $info = $reader->readRestOfPacketString();
        } catch (\Exception) {
            // Info string is optional
        }

        return new self($affectedRows, $lastInsertId, $statusFlags, $warnings, $info);
    }
}