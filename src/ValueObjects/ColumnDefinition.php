<?php

namespace Hibla\MysqlClient\ValueObjects;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final readonly class ColumnDefinition
{
    public function __construct(
        public string $catalog,
        public string $schema,
        public string $table,
        public string $orgTable,
        public string $name,
        public string $orgName,
        public int $charset,
        public int $length,
        public int $type,
        public int $flags,
        public int $decimals,
    ) {}

    public static function fromPayload(PayloadReader $reader): self
    {
        $catalog = $reader->readLengthEncodedStringOrNull();
        $schema = $reader->readLengthEncodedStringOrNull();
        $table = $reader->readLengthEncodedStringOrNull();
        $orgTable = $reader->readLengthEncodedStringOrNull();
        $name = $reader->readLengthEncodedStringOrNull();
        $orgName = $reader->readLengthEncodedStringOrNull();
        $reader->readLengthEncodedIntegerOrNull(); 
        $charset = $reader->readFixedInteger(2);
        $length = $reader->readFixedInteger(4);
        $type = $reader->readFixedInteger(1);
        $flags = $reader->readFixedInteger(2);
        $decimals = $reader->readFixedInteger(1);
        $reader->readFixedString(2); 
        
        return new self(
            catalog: $catalog,
            schema: $schema,
            table: $table,
            orgTable: $orgTable,
            name: $name,
            orgName: $orgName,
            charset: $charset,
            length: $length,
            type: $type,
            flags: $flags,
            decimals: $decimals,
        );
    }
}