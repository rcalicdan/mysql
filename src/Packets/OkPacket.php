<?php

namespace Hibla\MysqlClient\Packets;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class OkPacket
{
    public int $affectedRows;
    public int $lastInsertId;
    public int $statusFlags;
    public int $warnings;
    public string $info;

    public function __construct(int $affectedRows = 0, int $lastInsertId = 0, int $statusFlags = 0, int $warnings = 0, string $info = '')
    {
        $this->affectedRows = $affectedRows;
        $this->lastInsertId = $lastInsertId;
        $this->statusFlags = $statusFlags;
        $this->warnings = $warnings;
        $this->info = $info;
    }

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
        } catch (\Exception $e) {
            // Info string is optional
        }

        return new self($affectedRows, $lastInsertId, $statusFlags, $warnings, $info);
    }
}
