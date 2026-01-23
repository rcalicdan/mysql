<?php

namespace Hibla\MysqlClient\Protocols;

use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;

final class PacketBuilder
{
    private const MYSQL_TYPES = [
        'null' => 0x06,
        'double' => 0x05,
        'longlong' => 0x08,
        'var_string' => 0x0F,
    ];

    private const COMMANDS = [
        'quit' => "\x01",
        'query' => "\x03",
        'stmt_prepare' => "\x16",
        'stmt_execute' => "\x17",
        'stmt_close' => "\x19",
    ];

    private const DEFAULT_AUTH_PLUGIN = 'caching_sha2_password';

    private array $connectionParams;
    private int $clientCapabilities;

    public function __construct(array $connectionParams, int $clientCapabilities)
    {
        $this->connectionParams = $connectionParams;
        $this->clientCapabilities = $clientCapabilities;
    }

    public function buildHandshakeResponse(string $nonce): string
    {
        $user = $this->connectionParams['username'] ?? '';
        $password = $this->connectionParams['password'] ?? '';
        $database = $this->connectionParams['database'] ?? '';

        return $this->buildHandshakeHeader()
            .$user."\x00"
            .$this->buildPasswordSection($password, $nonce)
            .$this->buildDatabaseSection($database)
            .$this->buildAuthPluginSection();
    }

    public function buildQueryPacket(string $sql): string
    {
        return self::COMMANDS['query'].$sql;
    }

    public function buildQuitPacket(): string
    {
        return self::COMMANDS['quit'];
    }

    public function buildStmtPreparePacket(string $sql): string
    {
        return self::COMMANDS['stmt_prepare'].$sql;
    }

    public function buildStmtClosePacket(int $statementId): string
    {
        return self::COMMANDS['stmt_close'].pack('V', $statementId);
    }

    public function buildStmtExecutePacket(int $statementId, array $params): string
    {
        $packet = self::COMMANDS['stmt_execute']
            .pack('V', $statementId)    // Statement ID
            ."\x00"                     // Flags
            .pack('V', 1);              // Iteration count

        if (empty($params)) {
            return $packet;
        }

        $packet .= $this->buildNullBitmap($params)
            ."\x01"                     // New params bound flag
            .$this->buildParameterTypes($params)
            .$this->buildParameterValues($params);

        return $packet;
    }

    private function buildHandshakeHeader(): string
    {
        return pack('V', $this->clientCapabilities)
            .pack('V', 0x01000000)      // Max packet size
            .pack('C', 45)              // Charset
            .str_repeat("\x00", 23);    // Reserved bytes
    }

    private function buildPasswordSection(string $password, string $nonce): string
    {
        if ($password === '') {
            return "\x00";
        }

        $scrambledPassword = Auth::scrambleCachingSha2Password($password, $nonce);

        return pack('C', strlen($scrambledPassword)).$scrambledPassword;
    }

    private function buildDatabaseSection(string $database): string
    {
        return ($this->clientCapabilities & CapabilityFlags::CLIENT_CONNECT_WITH_DB) && $database !== ''
            ? $database."\x00"
            : '';
    }

    private function buildAuthPluginSection(): string
    {
        return ($this->clientCapabilities & CapabilityFlags::CLIENT_PLUGIN_AUTH)
            ? self::DEFAULT_AUTH_PLUGIN."\x00"
            : '';
    }

    private function buildNullBitmap(array $params): string
    {
        $numParams = count($params);
        $nullBitmapSize = (int) (($numParams + 7) / 8);
        $nullBitmap = str_repeat("\x00", $nullBitmapSize);

        foreach ($params as $i => $param) {
            if ($param === null) {
                $byteIndex = (int) ($i / 8);
                $bitIndex = $i % 8;
                $nullBitmap[$byteIndex] = chr(ord($nullBitmap[$byteIndex]) | (1 << $bitIndex));
            }
        }

        return $nullBitmap;
    }

    private function buildParameterTypes(array $params): string
    {
        $types = '';
        foreach ($params as $param) {
            $types .= pack('v', $this->getParameterType($param));
        }

        return $types;
    }

    private function buildParameterValues(array $params): string
    {
        $values = '';
        foreach ($params as $param) {
            $values .= $this->encodeParameterValue($param);
        }

        return $values;
    }

    private function getParameterType($param): int
    {
        if ($param === null) {
            return self::MYSQL_TYPES['null'];
        }
        if (is_int($param)) {
            return self::MYSQL_TYPES['longlong'];
        }
        if (is_float($param)) {
            return self::MYSQL_TYPES['double'];
        }

        return self::MYSQL_TYPES['var_string'];
    }

    private function encodeParameterValue($param): string
    {
        if ($param === null) {
            return '';
        }

        if (is_int($param)) {
            return pack('P', $param);
        }

        if (is_float($param)) {
            return pack('e', $param);
        }

        return $this->encodeLengthEncodedString((string) $param);
    }

    private function encodeLengthEncodedString(string $str): string
    {
        $len = strlen($str);

        return match (true) {
            $len < 251 => chr($len).$str,
            $len < 65536 => "\xfc".pack('v', $len).$str,
            $len < 16777216 => "\xfd".substr(pack('V', $len), 0, 3).$str,
            default => "\xfe".pack('P', $len).$str,
        };
    }
}
