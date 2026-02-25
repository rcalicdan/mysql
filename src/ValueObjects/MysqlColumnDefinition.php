<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

use Rcalicdan\MySQLBinaryProtocol\Constants\CharsetIdentifiers;
use Rcalicdan\MySQLBinaryProtocol\Constants\ColumnFlags;
use Rcalicdan\MySQLBinaryProtocol\Constants\MysqlType;
use Rcalicdan\MySQLBinaryProtocol\Frame\Result\ColumnDefinition;

/**
 * Represents a human-readable MySQL Column Definition.
 *
 * This Value Object wraps the raw binary protocol column definition and translates
 * raw integer identifiers and bitmasks into standard, human-readable MySQL types,
 * character sets, and boolean flags.
 */
final class MysqlColumnDefinition
{
    /**
     * The catalog used. This is almost always 'def'.
     */
    public readonly string $catalog;

    /**
     * The database (schema) name where the table resides.
     */
    public readonly string $schema;

    /**
     * The virtual table name (e.g., if an alias was used like `FROM users u`).
     */
    public readonly string $table;

    /**
     * The original, physical table name.
     */
    public readonly string $orgTable;

    /**
     * The virtual column name (e.g., if an alias was used like `SELECT id AS user_id`).
     */
    public readonly string $name;

    /**
     * The original, physical column name.
     */
    public readonly string $orgName;

    /**
     * The raw numeric character set ID (collation) of the column.
     */
    public readonly int $charset;

    /**
     * The maximum length of the field in bytes (not characters).
     */
    public readonly int $columnLength;

    /**
     * The raw bitmask of column flags (e.g., NOT NULL, PRIMARY KEY).
     */
    public readonly int $flags;

    /**
     * The maximum number of decimal digits (for numeric types).
     * For integers or strings, this is typically 0.
     */
    public readonly int $decimals;

    /**
     * The resolved, human-readable MySQL data type (e.g., 'VARCHAR', 'INT UNSIGNED').
     */
    public readonly string $typeName;

    /**
     * The resolved, human-readable character set name (e.g., 'utf8mb4', 'latin1').
     */
    public readonly string $charsetName;

    /**
     * The calculated maximum character length (adjusted for multibyte character sets).
     */
    public readonly int $length;

    /**
     * A list of human-readable flags applied to this column (e.g., ['NOT NULL', 'PRIMARY KEY', 'AUTO INCREMENT']).
     *
     * @var list<string>
     */
    public readonly array $resolvedFlags;

    /**
     * Constructs a human-readable column definition from the raw protocol frame.
     *
     * @param ColumnDefinition $raw The raw column definition from the binary protocol.
     */
    public function __construct(ColumnDefinition $raw)
    {
        $this->catalog = $raw->catalog;
        $this->schema = $raw->schema;
        $this->table = $raw->table;
        $this->orgTable = $raw->orgTable;
        $this->name = $raw->name;
        $this->orgName = $raw->orgName;
        $this->charset = $raw->charset;
        $this->columnLength = $raw->columnLength;
        // Map the raw property to the original type identifier (kept locally for resolution)
        $rawType = $raw->type;
        $this->flags = $raw->flags;
        $this->decimals = $raw->decimals;

        $this->typeName = self::resolveType($rawType, $raw->flags);
        $this->charsetName = self::resolveCharset($raw->charset);
        $this->length = self::resolveCharLength($raw->columnLength, $raw->charset);
        $this->resolvedFlags = self::resolveFlags($raw->flags);
    }

    /**
     * Determines if the column accepts NULL values.
     */
    public function isNullable(): bool
    {
        return ($this->flags & ColumnFlags::NOT_NULL_FLAG) === 0;
    }

    /**
     * Determines if the column is a Primary Key.
     */
    public function isPrimaryKey(): bool
    {
        return ($this->flags & ColumnFlags::PRI_KEY_FLAG) !== 0;
    }

    /**
     * Determines if the column is part of a Unique Key.
     */
    public function isUniqueKey(): bool
    {
        return ($this->flags & ColumnFlags::UNIQUE_KEY_FLAG) !== 0;
    }

    /**
     * Determines if the column is an unsigned numeric type.
     */
    public function isUnsigned(): bool
    {
        return ($this->flags & ColumnFlags::UNSIGNED_FLAG) !== 0;
    }

    /**
     * Determines if the column uses the AUTO_INCREMENT attribute.
     */
    public function isAutoIncrement(): bool
    {
        return ($this->flags & 0x200) !== 0;
    }

    /**
     * Determines if the column contains binary data (e.g., BINARY, VARBINARY).
     */
    public function isBinary(): bool
    {
        return ($this->flags & ColumnFlags::BINARY_FLAG) !== 0;
    }

    /**
     * Determines if the column is a BLOB or TEXT type.
     */
    public function isBlob(): bool
    {
        return ($this->flags & ColumnFlags::BLOB_FLAG) !== 0;
    }

    /**
     * Resolves the actual character length based on byte length and charset.
     */
    private static function resolveCharLength(int $byteLength, int $charset): int
    {
        $bytesPerChar = match ($charset) {
            CharsetIdentifiers::UTF8MB4 => 4,
            CharsetIdentifiers::UTF8 => 3,
            CharsetIdentifiers::LATIN1 => 1,
            default => 1,
        };

        return (int) ($byteLength / $bytesPerChar);
    }

    /**
     * Maps a character set identifier to its human-readable name.
     */
    private static function resolveCharset(int $charset): string
    {
        return match ($charset) {
            CharsetIdentifiers::UTF8MB4 => 'utf8mb4',
            CharsetIdentifiers::UTF8 => 'utf8',
            CharsetIdentifiers::LATIN1 => 'latin1',
            63 => 'binary',
            default => 'unknown(' . $charset . ')',
        };
    }

    /**
     * Maps the raw protocol type and flags into a standard MySQL type string.
     */
    private static function resolveType(int $type, int $flags): string
    {
        $unsigned = ($flags & ColumnFlags::UNSIGNED_FLAG) !== 0;

        return match ($type) {
            MysqlType::DECIMAL => 'DECIMAL',
            MysqlType::TINY => $unsigned ? 'TINYINT UNSIGNED' : 'TINYINT',
            MysqlType::SHORT => $unsigned ? 'SMALLINT UNSIGNED' : 'SMALLINT',
            MysqlType::LONG => $unsigned ? 'INT UNSIGNED' : 'INT',
            MysqlType::FLOAT => 'FLOAT',
            MysqlType::DOUBLE => 'DOUBLE',
            MysqlType::NULL => 'NULL',
            MysqlType::TIMESTAMP => 'TIMESTAMP',
            MysqlType::LONGLONG => $unsigned ? 'BIGINT UNSIGNED' : 'BIGINT',
            MysqlType::INT24 => $unsigned ? 'MEDIUMINT UNSIGNED' : 'MEDIUMINT',
            MysqlType::DATE => 'DATE',
            MysqlType::TIME => 'TIME',
            MysqlType::DATETIME => 'DATETIME',
            MysqlType::YEAR => 'YEAR',
            MysqlType::VARCHAR => 'VARCHAR',
            MysqlType::BIT => 'BIT',
            MysqlType::JSON => 'JSON',
            MysqlType::NEWDECIMAL => 'DECIMAL',
            MysqlType::ENUM => 'ENUM',
            MysqlType::SET => 'SET',
            MysqlType::TINY_BLOB => 'TINYBLOB',
            MysqlType::MEDIUM_BLOB => 'MEDIUMBLOB',
            MysqlType::LONG_BLOB => 'LONGBLOB',
            MysqlType::BLOB => 'BLOB',
            MysqlType::VAR_STRING => 'VARCHAR',
            MysqlType::STRING => self::resolveStringType($flags),
            MysqlType::GEOMETRY => 'GEOMETRY',
            default => 'UNKNOWN(0x' . dechex($type) . ')',
        };
    }

    /**
     * Resolves the specific string type (CHAR, ENUM, SET) from the flags.
     * MySQL represents these identically over the wire, differentiating via flags.
     */
    private static function resolveStringType(int $flags): string
    {
        if (($flags & 0x100) !== 0) {
            return 'ENUM';
        }

        if (($flags & 0x800) !== 0) {
            return 'SET';
        }

        return 'CHAR';
    }

    /**
     * Extracts a list of human-readable property labels from the flag bitmask.
     *
     * @return list<string>
     */
    private static function resolveFlags(int $flags): array
    {
        $resolved = [];

        $map = [
            ColumnFlags::NOT_NULL_FLAG => 'NOT NULL',
            ColumnFlags::PRI_KEY_FLAG => 'PRIMARY KEY',
            ColumnFlags::UNIQUE_KEY_FLAG => 'UNIQUE KEY',
            ColumnFlags::MULTIPLE_KEY_FLAG => 'KEY',
            ColumnFlags::BLOB_FLAG => 'BLOB',
            ColumnFlags::UNSIGNED_FLAG => 'UNSIGNED',
            ColumnFlags::ZEROFILL_FLAG => 'ZEROFILL',
            ColumnFlags::BINARY_FLAG => 'BINARY',
            0x100 => 'ENUM',
            0x200 => 'AUTO INCREMENT',
            0x400 => 'TIMESTAMP',
            0x800 => 'SET',
            0x1000 => 'NO DEFAULT',
            0x4000 => 'NUMERIC',
        ];

        foreach ($map as $bit => $label) {
            if (($flags & $bit) !== 0) {
                $resolved[] = $label;
            }
        }

        return $resolved;
    }
}
