<?php

namespace Hibla\MysqlClient\Protocols;

use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;

final class ColumnDefinition
{
    public string $catalog;
    public string $schema;
    public string $table;
    public string $orgTable;
    public string $name;
    public string $orgName;
    public int $charset;
    public int $length;
    public int $type;
    public int $flags;
    public int $decimals;

    public static function fromPayload(PayloadReader $reader): self
    {
        $col = new self;
        $col->catalog = $reader->readLengthEncodedStringOrNull();
        $col->schema = $reader->readLengthEncodedStringOrNull();
        $col->table = $reader->readLengthEncodedStringOrNull();
        $col->orgTable = $reader->readLengthEncodedStringOrNull();
        $col->name = $reader->readLengthEncodedStringOrNull();
        $col->orgName = $reader->readLengthEncodedStringOrNull();
        $reader->readLengthEncodedIntegerOrNull(); // length of fixed-length fields, always 0x0c
        $col->charset = $reader->readFixedInteger(2);
        $col->length = $reader->readFixedInteger(4);
        $col->type = $reader->readFixedInteger(1);
        $col->flags = $reader->readFixedInteger(2);
        $col->decimals = $reader->readFixedInteger(1);
        $reader->readFixedString(2); // filler

        return $col;
    }
}
