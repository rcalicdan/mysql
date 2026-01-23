<?php

use Hibla\MysqlClient\Enums\Command;
use Hibla\MysqlClient\Enums\MysqlType;
use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;

describe('PacketBuilder', function () {
    describe('buildQueryPacket', function () {
        it('builds query packet with correct command byte', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildQueryPacket('SELECT 1');

            expect($packet[0])->toBe(Command::QUERY->value)
                ->and(substr($packet, 1))->toBe('SELECT 1');
        });

        it('builds query packet with complex SQL', function () {
            $builder = createPacketBuilder();
            $sql = 'SELECT * FROM users WHERE id = 1';
            $packet = $builder->buildQueryPacket($sql);

            expect($packet)->toBe(Command::QUERY->value . $sql);
        });

        it('handles empty query', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildQueryPacket('');

            expect($packet)->toBe(Command::QUERY->value);
        });
    });

    describe('buildQuitPacket', function () {
        it('builds quit packet with correct command byte', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildQuitPacket();

            expect($packet)->toBe(Command::QUIT->value)
                ->and(strlen($packet))->toBe(1);
        });
    });

    describe('buildStmtPreparePacket', function () {
        it('builds prepare packet with correct command byte', function () {
            $builder = createPacketBuilder();
            $sql = 'SELECT * FROM users WHERE id = ?';
            $packet = $builder->buildStmtPreparePacket($sql);

            expect($packet[0])->toBe(Command::STMT_PREPARE->value)
                ->and(substr($packet, 1))->toBe($sql);
        });
    });

    describe('buildStmtClosePacket', function () {
        it('builds close packet with statement id', function () {
            $builder = createPacketBuilder();
            $statementId = 12345;
            $packet = $builder->buildStmtClosePacket($statementId);

            expect($packet[0])->toBe(Command::STMT_CLOSE->value);

            $unpackedId = unpack('V', substr($packet, 1))[1];
            expect($unpackedId)->toBe($statementId);
        });

        it('handles zero statement id', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtClosePacket(0);

            $unpackedId = unpack('V', substr($packet, 1))[1];
            expect($unpackedId)->toBe(0);
        });
    });

    describe('buildStmtExecutePacket', function () {
        it('builds execute packet without parameters', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $packet = $builder->buildStmtExecutePacket($statementId, []);

            expect($packet[0])->toBe(Command::STMT_EXECUTE->value);

            $unpackedId = unpack('V', substr($packet, 1, 4))[1];
            expect($unpackedId)->toBe($statementId);

            expect($packet[5])->toBe("\x00");

            $iterationCount = unpack('V', substr($packet, 6, 4))[1];
            expect($iterationCount)->toBe(1);

            expect(strlen($packet))->toBe(10);
        });

        it('builds execute packet with integer parameter', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $params = [42];
            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            expect(strlen($packet))->toBeGreaterThan(10);

            $nullBitmapSize = (int)((count($params) + 7) / 8);
            $newParamsBoundFlag = $packet[10 + $nullBitmapSize];
            expect($newParamsBoundFlag)->toBe("\x01");
        });

        it('builds execute packet with null parameter', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $params = [null];
            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $nullBitmap = $packet[10];
            expect(ord($nullBitmap) & 0x01)->toBe(1);
        });

        it('builds execute packet with multiple parameters of different types', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $params = [42, 3.14, 'hello', null];
            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            expect($packet[0])->toBe(Command::STMT_EXECUTE->value);

            $nullBitmap = $packet[10];
            expect(ord($nullBitmap) & 0x08)->toBe(8);
        });

        it('builds null bitmap correctly for 8 parameters', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $params = [1, null, 3, null, 5, null, 7, null];
            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $nullBitmap = $packet[10];
            expect(ord($nullBitmap))->toBe(0xAA);
        });

        it('builds null bitmap correctly for 9 parameters requiring 2 bytes', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $params = [1, 2, 3, 4, 5, 6, 7, 8, null];
            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $nullBitmap1 = ord($packet[10]);
            $nullBitmap2 = ord($packet[11]);

            expect($nullBitmap1)->toBe(0x00)
                ->and($nullBitmap2)->toBe(0x01);
        });

        it('encodes string parameter with correct length', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $params = ['test'];
            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $stringStart = 14;
            $length = ord($packet[$stringStart]);

            expect($length)->toBe(4)
                ->and(substr($packet, $stringStart + 1, 4))->toBe('test');
        });
    });

    describe('buildHandshakeResponse', function () {
        it('builds handshake with username', function () {
            $builder = createPacketBuilder();
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            $capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_PLUGIN_AUTH
                | CapabilityFlags::CLIENT_CONNECT_WITH_DB;

            $packetCapabilities = unpack('V', substr($packet, 0, 4))[1];
            expect($packetCapabilities)->toBe($capabilities);

            $maxPacket = unpack('V', substr($packet, 4, 4))[1];
            expect($maxPacket)->toBe(0x01000000);

            $charset = ord($packet[8]);
            expect($charset)->toBe(45);

            expect(substr($packet, 9, 23))->toBe(str_repeat("\x00", 23));

            expect(substr($packet, 32, 8))->toBe('testuser');
            expect($packet[40])->toBe("\x00");
        });

        it('builds handshake with empty password', function () {
            $builder = createPacketBuilder(password: '');
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            $usernameEnd = 32 + strlen('testuser') + 1;
            expect($packet[$usernameEnd])->toBe("\x00");
        });

        it('builds handshake without database when not in capabilities', function () {
            $capabilitiesWithoutDb = CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_PLUGIN_AUTH;

            $builder = createPacketBuilder(capabilities: $capabilitiesWithoutDb);
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, 'testdb'))->toBeFalse();
        });

        it('builds handshake with database when capability is set', function () {
            $builder = createPacketBuilder();
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, 'testdb'))->toBeGreaterThan(0);
        });

        it('builds handshake with auth plugin name', function () {
            $builder = createPacketBuilder();
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, 'caching_sha2_password'))->toBeGreaterThan(0);
        });

        it('builds handshake without auth plugin when capability not set', function () {
            $capabilitiesWithoutAuth = CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_CONNECT_WITH_DB;

            $builder = createPacketBuilder(capabilities: $capabilitiesWithoutAuth);
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, 'caching_sha2_password'))->toBeFalse();
        });
    });

    describe('length encoded string encoding', function () {
        it('encodes short string with single byte length', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $shortString = 'abc';
            $params = [$shortString];

            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $stringStart = 14;

            expect(ord($packet[$stringStart]))->toBe(3)
                ->and(substr($packet, $stringStart + 1, 3))->toBe('abc');
        });

        it('encodes medium string with 0xfc prefix', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $mediumString = str_repeat('a', 300);
            $params = [$mediumString];

            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $stringStart = 14;

            expect(ord($packet[$stringStart]))->toBe(0xfc);

            $length = unpack('v', substr($packet, $stringStart + 1, 2))[1];
            expect($length)->toBe(300);
        });

        it('encodes large string with 0xfd prefix', function () {
            $builder = createPacketBuilder();
            $statementId = 1;
            $largeString = str_repeat('a', 70000);
            $params = [$largeString];

            $packet = $builder->buildStmtExecutePacket($statementId, $params);

            $stringStart = 14;

            expect(ord($packet[$stringStart]))->toBe(0xfd);

            $lengthBytes = substr($packet, $stringStart + 1, 3) . "\x00";
            $length = unpack('V', $lengthBytes)[1];
            expect($length)->toBe(70000);
        });
    });

    describe('parameter type detection', function () {
        it('detects integer type', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [42]);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];

            expect($type)->toBe(MysqlType::LONGLONG->value);
        });

        it('detects double type', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [3.14]);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];

            expect($type)->toBe(MysqlType::DOUBLE->value);
        });

        it('detects string type', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, ['hello']);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];

            expect($type)->toBe(MysqlType::VAR_STRING->value);
        });

        it('detects null type', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [null]);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];

            expect($type)->toBe(MysqlType::NULL->value);
        });
    });
    describe('buildQueryPacket - additional edge cases', function () {
        it('handles SQL with special characters', function () {
            $builder = createPacketBuilder();
            $sql = "SELECT * FROM users WHERE name = 'O''Brien'";
            $packet = $builder->buildQueryPacket($sql);

            expect(substr($packet, 1))->toBe($sql);
        });

        it('handles SQL with newlines and tabs', function () {
            $builder = createPacketBuilder();
            $sql = "SELECT *\nFROM users\n\tWHERE id = 1";
            $packet = $builder->buildQueryPacket($sql);

            expect(substr($packet, 1))->toBe($sql);
        });

        it('handles very long SQL query', function () {
            $builder = createPacketBuilder();
            $sql = 'SELECT * FROM users WHERE id IN (' . implode(',', range(1, 10000)) . ')';
            $packet = $builder->buildQueryPacket($sql);

            expect($packet[0])->toBe(Command::QUERY->value)
                ->and(strlen($packet))->toBe(strlen($sql) + 1);
        });

        it('handles SQL with null bytes', function () {
            $builder = createPacketBuilder();
            $sql = "SELECT * FROM users WHERE data = '\x00'";
            $packet = $builder->buildQueryPacket($sql);

            expect(substr($packet, 1))->toBe($sql);
        });

        it('handles SQL with unicode characters', function () {
            $builder = createPacketBuilder();
            $sql = "SELECT * FROM users WHERE name = '你好世界'";
            $packet = $builder->buildQueryPacket($sql);

            expect(substr($packet, 1))->toBe($sql);
        });
    });

    describe('buildStmtExecutePacket - additional edge cases', function () {
        it('handles maximum integer value', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [PHP_INT_MAX]);

            expect($packet[0])->toBe(Command::STMT_EXECUTE->value);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];
            expect($type)->toBe(MysqlType::LONGLONG->value);
        });

        it('handles minimum integer value', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [PHP_INT_MIN]);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];
            expect($type)->toBe(MysqlType::LONGLONG->value);
        });

        it('handles negative integers', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [-42, -100, -1]);

            $typeStart = 12;
            $type1 = unpack('v', substr($packet, $typeStart, 2))[1];
            $type2 = unpack('v', substr($packet, $typeStart + 2, 2))[1];
            $type3 = unpack('v', substr($packet, $typeStart + 4, 2))[1];

            expect($type1)->toBe(MysqlType::LONGLONG->value)
                ->and($type2)->toBe(MysqlType::LONGLONG->value)
                ->and($type3)->toBe(MysqlType::LONGLONG->value);
        });

        it('handles zero as parameter', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [0]);

            $typeStart = 12;
            $type = unpack('v', substr($packet, $typeStart, 2))[1];
            expect($type)->toBe(MysqlType::LONGLONG->value);
        });

        it('handles float special values', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [0.0, -0.0, 1.7e308]);

            $typeStart = 12;
            $type1 = unpack('v', substr($packet, $typeStart, 2))[1];
            $type2 = unpack('v', substr($packet, $typeStart + 2, 2))[1];
            $type3 = unpack('v', substr($packet, $typeStart + 4, 2))[1];

            expect($type1)->toBe(MysqlType::DOUBLE->value)
                ->and($type2)->toBe(MysqlType::DOUBLE->value)
                ->and($type3)->toBe(MysqlType::DOUBLE->value);
        });

        it('handles negative floats', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, [-3.14, -0.001]);

            $typeStart = 12;
            $type1 = unpack('v', substr($packet, $typeStart, 2))[1];
            $type2 = unpack('v', substr($packet, $typeStart + 2, 2))[1];

            expect($type1)->toBe(MysqlType::DOUBLE->value)
                ->and($type2)->toBe(MysqlType::DOUBLE->value);
        });

        it('handles empty string parameter', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, ['']);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(0);
        });

        it('handles string with null bytes', function () {
            $builder = createPacketBuilder();
            $stringWithNull = "hello\x00world";
            $packet = $builder->buildStmtExecutePacket(1, [$stringWithNull]);

            $stringStart = 14;
            $length = ord($packet[$stringStart]);
            expect($length)->toBe(11)
                ->and(substr($packet, $stringStart + 1, 11))->toBe($stringWithNull);
        });

        it('handles unicode string parameter', function () {
            $builder = createPacketBuilder();
            $unicode = '你好世界🌍';
            $packet = $builder->buildStmtExecutePacket(1, [$unicode]);

            $stringStart = 14;
            $length = ord($packet[$stringStart]);
            expect(substr($packet, $stringStart + 1, $length))->toBe($unicode);
        });

        it('handles string exactly 250 bytes (boundary)', function () {
            $builder = createPacketBuilder();
            $string250 = str_repeat('a', 250);
            $packet = $builder->buildStmtExecutePacket(1, [$string250]);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(250)
                ->and(substr($packet, $stringStart + 1, 250))->toBe($string250);
        });

        it('handles string exactly 251 bytes (boundary requiring 0xfc)', function () {
            $builder = createPacketBuilder();
            $string251 = str_repeat('a', 251);
            $packet = $builder->buildStmtExecutePacket(1, [$string251]);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(0xfc);

            $length = unpack('v', substr($packet, $stringStart + 1, 2))[1];
            expect($length)->toBe(251);
        });

        it('handles string exactly 65535 bytes (boundary)', function () {
            $builder = createPacketBuilder();
            $string65535 = str_repeat('a', 65535);
            $packet = $builder->buildStmtExecutePacket(1, [$string65535]);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(0xfc);

            $length = unpack('v', substr($packet, $stringStart + 1, 2))[1];
            expect($length)->toBe(65535);
        });

        it('handles string exactly 65536 bytes (boundary requiring 0xfd)', function () {
            $builder = createPacketBuilder();
            $string65536 = str_repeat('a', 65536);
            $packet = $builder->buildStmtExecutePacket(1, [$string65536]);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(0xfd);
        });

        it('handles null bitmap for 16 parameters', function () {
            $builder = createPacketBuilder();
            $params = array_fill(0, 16, 1);
            $params[0] = null;
            $params[15] = null;
            $packet = $builder->buildStmtExecutePacket(1, $params);

            $nullBitmap1 = ord($packet[10]);
            $nullBitmap2 = ord($packet[11]);

            expect($nullBitmap1)->toBe(0x01)
                ->and($nullBitmap2)->toBe(0x80);
        });

        it('handles all null parameters', function () {
            $builder = createPacketBuilder();
            $params = [null, null, null, null];
            $packet = $builder->buildStmtExecutePacket(1, $params);

            $nullBitmap = ord($packet[10]);
            expect($nullBitmap)->toBe(0x0F);
        });

        it('handles mixed parameter types in specific order', function () {
            $builder = createPacketBuilder();
            $params = [null, 0, '', -1, 0.0, 'test'];
            $packet = $builder->buildStmtExecutePacket(1, $params);

            expect($packet[0])->toBe(Command::STMT_EXECUTE->value);

            $nullBitmap = ord($packet[10]);
            expect($nullBitmap & 0x01)->toBe(1);
        });

        it('handles maximum statement id', function () {
            $builder = createPacketBuilder();
            $maxStatementId = 0xFFFFFFFF;
            $packet = $builder->buildStmtExecutePacket($maxStatementId, []);

            $unpackedId = unpack('V', substr($packet, 1, 4))[1];
            expect($unpackedId)->toBe($maxStatementId);
        });

        it('handles large number of parameters', function () {
            $builder = createPacketBuilder();
            $params = array_fill(0, 100, 'test');
            $packet = $builder->buildStmtExecutePacket(1, $params);

            expect($packet[0])->toBe(Command::STMT_EXECUTE->value);

            $nullBitmapSize = (int)((count($params) + 7) / 8);
            expect($nullBitmapSize)->toBe(13);
        });
    });

    describe('buildStmtClosePacket - additional edge cases', function () {
        it('handles maximum statement id', function () {
            $builder = createPacketBuilder();
            $maxId = 0xFFFFFFFF;
            $packet = $builder->buildStmtClosePacket($maxId);

            $unpackedId = unpack('V', substr($packet, 1))[1];
            expect($unpackedId)->toBe($maxId);
        });

        it('verifies packet structure', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtClosePacket(100);

            expect(strlen($packet))->toBe(5)
                ->and($packet[0])->toBe(Command::STMT_CLOSE->value);
        });
    });

    describe('buildStmtPreparePacket - additional edge cases', function () {
        it('handles SQL with placeholders at boundaries', function () {
            $builder = createPacketBuilder();
            $sql = '? AND col = ? AND ?';
            $packet = $builder->buildStmtPreparePacket($sql);

            expect(substr($packet, 1))->toBe($sql);
        });

        it('handles very long SQL with many placeholders', function () {
            $builder = createPacketBuilder();
            $placeholders = implode(', ', array_fill(0, 100, '?'));
            $sql = "INSERT INTO users (col1) VALUES ($placeholders)";
            $packet = $builder->buildStmtPreparePacket($sql);

            expect(substr($packet, 1))->toBe($sql);
        });

        it('handles empty SQL', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtPreparePacket('');

            expect($packet)->toBe(Command::STMT_PREPARE->value);
        });
    });

    describe('buildHandshakeResponse - additional edge cases', function () {
        it('handles very long username', function () {
            $longUsername = str_repeat('a', 100);
            $builder = createPacketBuilder(username: $longUsername);
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, $longUsername))->toBeGreaterThan(0);
        });

        it('handles username with special characters', function () {
            $specialUsername = "user@host-name_123";
            $builder = createPacketBuilder(username: $specialUsername);
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, $specialUsername))->toBeGreaterThan(0);
        });

        it('handles very long database name', function () {
            $longDatabase = str_repeat('d', 64);
            $builder = createPacketBuilder(database: $longDatabase);
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect(strpos($packet, $longDatabase))->toBeGreaterThan(0);
        });

        it('handles non-standard nonce length', function () {
            $builder = createPacketBuilder();
            $shortNonce = str_repeat("\x00", 10);
            $packet = $builder->buildHandshakeResponse($shortNonce);

            expect($packet)->toBeString();
        });

        it('handles nonce with special bytes', function () {
            $builder = createPacketBuilder();
            $nonce = str_repeat("\xFF", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            expect($packet)->toBeString();
        });

        it('verifies charset is always 45', function () {
            $builder = createPacketBuilder();
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            $charset = ord($packet[8]);
            expect($charset)->toBe(45);
        });

        it('verifies max packet size is always 16MB', function () {
            $builder = createPacketBuilder();
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            $maxPacket = unpack('V', substr($packet, 4, 4))[1];
            expect($maxPacket)->toBe(0x01000000);
        });

        it('handles all capability flags combinations', function () {
            $caps = CapabilityFlags::CLIENT_PROTOCOL_41;
            $builder = createPacketBuilder(capabilities: $caps);
            $nonce = str_repeat("\x00", 20);
            $packet = $builder->buildHandshakeResponse($nonce);

            $packetCapabilities = unpack('V', substr($packet, 0, 4))[1];
            expect($packetCapabilities)->toBe($caps);
        });
    });

    describe('length encoded string encoding - boundary tests', function () {
        it('encodes string of exactly 0 bytes', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, ['']);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(0);
        });

        it('encodes string of exactly 1 byte', function () {
            $builder = createPacketBuilder();
            $packet = $builder->buildStmtExecutePacket(1, ['a']);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(1)
                ->and($packet[$stringStart + 1])->toBe('a');
        });

        it('encodes string of exactly 250 bytes', function () {
            $builder = createPacketBuilder();
            $string = str_repeat('x', 250);
            $packet = $builder->buildStmtExecutePacket(1, [$string]);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(250);
        });

        it('encodes string of exactly 16777215 bytes (max for 3-byte)', function () {
            $builder = createPacketBuilder();
            $string = str_repeat('x', 16777215);
            $packet = $builder->buildStmtExecutePacket(1, [$string]);

            $stringStart = 14;
            expect(ord($packet[$stringStart]))->toBe(0xfd);

            $lengthBytes = substr($packet, $stringStart + 1, 3) . "\x00";
            $length = unpack('V', $lengthBytes)[1];
            expect($length)->toBe(16777215);
        });
    });
});
