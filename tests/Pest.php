<?php

use Hibla\MysqlClient\Enums\FieldType;
use Hibla\MysqlClient\Protocols\PacketBuilder;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;

function createPacketBuilder(
    string $username = 'testuser',
    string $password = 'testpass',
    string $database = 'testdb',
    ?int $capabilities = null
): PacketBuilder {
    $params = new ConnectionParams(
        username: $username,
        password: $password,
        database: $database
    );

    $caps = $capabilities ?? (
        CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
    );

    return new PacketBuilder($params, $caps);
}

function buildMockColumnPacket(string $name = 'test_col', int|FieldType $type = FieldType::LONG): string
{
    $typeValue = $type instanceof FieldType ? $type->value : $type;

    // catalog (length-encoded string)
    $packet = "\x03def";

    // schema
    $packet .= "\x04test";

    // table
    $packet .= "\x05users";

    // org_table
    $packet .= "\x05users";

    // name
    $packet .= pack('C', strlen($name)) . $name;

    // org_name
    $packet .= pack('C', strlen($name)) . $name;

    // fixed length fields (0x0c)
    $packet .= "\x0c";

    // character set (2 bytes)
    $packet .= pack('v', 33);

    // column length (4 bytes)
    $packet .= pack('V', 11);

    // type (1 byte)
    $packet .= pack('C', $typeValue);

    // flags (2 bytes)
    $packet .= pack('v', 0);

    // decimals (1 byte)
    $packet .= "\x00";

    // filler (2 bytes)
    $packet .= "\x00\x00";

    return $packet;
}

function buildMockRowPacket(int $columnCount, array $columnTypes = [], array $values = []): string
{
    $encodeVarString = function (string $value): string {
        $length = strlen($value);
        if ($length < 251) {
            return pack('C', $length) . $value;
        } elseif ($length < 65536) {
            return "\xfc" . pack('v', $length) . $value;
        } else {
            return "\xfd" . substr(pack('V', $length), 0, 3) . $value;
        }
    };

    $packet = "\x00";

    $nullBitmapSize = (int) floor(($columnCount + 7) / 8);
    $nullBitmap = str_repeat("\x00", $nullBitmapSize);
    $packet .= $nullBitmap;

    for ($i = 0; $i < $columnCount; $i++) {
        $type = $columnTypes[$i] ?? FieldType::LONG;
        $value = $values[$i] ?? ($i + 1) * 100;

        $typeValue = $type instanceof FieldType ? $type->value : $type;

        $packet .= match ($typeValue) {
            FieldType::TINY->value => pack('C', is_int($value) ? $value : 1),
            FieldType::SHORT->value => pack('v', is_int($value) ? $value : 100),
            FieldType::LONG->value => pack('V', is_int($value) ? $value : 1000),
            FieldType::LONGLONG->value => pack('P', is_int($value) ? $value : 10000),
            FieldType::YEAR->value => pack('V', is_int($value) ? $value : 2024),
            FieldType::FLOAT->value => pack('f', is_float($value) ? $value : 3.14),
            FieldType::DOUBLE->value => pack('d', is_float($value) ? $value : 3.14159),
            FieldType::MEDIUM->value => substr(pack('V', is_int($value) ? $value : 1000), 0, 3),
            FieldType::DECIMAL->value,
            FieldType::NEWDECIMAL->value,
            FieldType::ENUM->value,
            FieldType::SET->value,
            FieldType::JSON->value,
            FieldType::TINY_BLOB->value,
            FieldType::BLOB->value,
            FieldType::MEDIUM_BLOB->value,
            FieldType::LONG_BLOB->value,
            FieldType::STRING->value,
            FieldType::VAR_STRING->value => $encodeVarString(is_string($value) ? $value : "test{$i}"),
            default => $encodeVarString(is_string($value) ? $value : "value{$i}"),
        };
    }

    return $packet;
}

function buildMockTextRowPacket(array $values): string
{
    $packet = '';
    
    foreach ($values as $value) {
        if ($value === null) {
            $packet .= "\xfb";
        } else {
            $length = strlen($value);
            if ($length < 251) {
                $packet .= pack('C', $length) . $value;
            } elseif ($length < 65536) {
                $packet .= "\xfc" . pack('v', $length) . $value;
            } else {
                $packet .= "\xfd" . substr(pack('V', $length), 0, 3) . $value;
            }
        }
    }
    
    return $packet;
}
