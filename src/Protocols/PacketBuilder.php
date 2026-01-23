<?php

namespace Hibla\MysqlClient\Protocols;

use Hibla\MysqlClient\Enums\Command;
use Hibla\MysqlClient\Enums\MysqlType;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;

final class PacketBuilder
{
    private const string DEFAULT_AUTH_PLUGIN = 'caching_sha2_password';

    public function __construct(
        private readonly ConnectionParams $connectionParams,
        private readonly int $clientCapabilities
    ) {}

    public function buildHandshakeResponse(string $nonce): string
    {
        return $this->buildHandshakeHeader()
            . $this->connectionParams->username . "\x00"
            . $this->buildPasswordSection($nonce)
            . $this->buildDatabaseSection()
            . $this->buildAuthPluginSection();
    }

    public function buildQueryPacket(string $sql): string
    {
        return Command::QUERY->value . $sql;
    }

    public function buildQuitPacket(): string
    {
        return Command::QUIT->value;
    }

    public function buildStmtPreparePacket(string $sql): string
    {
        return Command::STMT_PREPARE->value . $sql;
    }

    public function buildStmtClosePacket(int $statementId): string
    {
        return Command::STMT_CLOSE->value . pack('V', $statementId);
    }

    public function buildStmtExecutePacket(int $statementId, array $params): string
    {
        $packet = Command::STMT_EXECUTE->value
            . pack('V', $statementId)
            . "\x00"
            . pack('V', 1);

        if (empty($params)) {
            return $packet;
        }

        $packet .= $this->buildNullBitmap($params)
            . "\x01"
            . $this->buildParameterTypes($params)
            . $this->buildParameterValues($params);

        return $packet;
    }

    private function buildHandshakeHeader(): string
    {
        return pack('V', $this->clientCapabilities)
            . pack('V', 0x01000000)
            . pack('C', 45)
            . str_repeat("\x00", 23);
    }

    private function buildPasswordSection(string $nonce): string
    {
        if (!$this->connectionParams->hasPassword()) {
            return "\x00";
        }

        $scrambledPassword = Auth::scrambleCachingSha2Password(
            $this->connectionParams->password,
            $nonce
        );

        return pack('C', strlen($scrambledPassword)) . $scrambledPassword;
    }

    private function buildDatabaseSection(): string
    {
        return ($this->clientCapabilities & CapabilityFlags::CLIENT_CONNECT_WITH_DB)
            && $this->connectionParams->hasDatabase()
            ? $this->connectionParams->database . "\x00"
            : '';
    }

    private function buildAuthPluginSection(): string
    {
        return ($this->clientCapabilities & CapabilityFlags::CLIENT_PLUGIN_AUTH)
            ? self::DEFAULT_AUTH_PLUGIN . "\x00"
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
            $types .= pack('v', $this->getParameterType($param)->value);
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

    private function getParameterType(mixed $param): MysqlType
    {
        if ($param === null) {
            return MysqlType::NULL;
        }
        if (\is_int($param)) {
            return MysqlType::LONGLONG;
        }
        if (\is_float($param)) {
            return MysqlType::DOUBLE;
        }

        return MysqlType::VAR_STRING;
    }

    private function encodeParameterValue(mixed $param): string
    {
        if ($param === null) {
            return '';
        }

        if (\is_int($param)) {
            return pack('P', $param);
        }

        if (\is_float($param)) {
            return pack('e', $param);
        }

        return $this->encodeLengthEncodedString((string) $param);
    }

    private function encodeLengthEncodedString(string $str): string
    {
        $len = \strlen($str);

        return match (true) {
            $len < 251 => \chr($len) . $str,
            $len < 65536 => "\xfc" . pack('v', $len) . $str,
            $len < 16777216 => "\xfd" . substr(pack('V', $len), 0, 3) . $str,
            default => "\xfe" . pack('P', $len) . $str,
        };
    }
}
