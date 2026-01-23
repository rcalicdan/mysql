<?php

use Hibla\MysqlClient\Enums\FieldType;
use Hibla\MysqlClient\Protocols\BinaryResultSetParser;
use Hibla\MysqlClient\ValueObjects\Result;

describe('BinaryResultSetParser', function () {
    describe('initial state', function () {
        it('starts with incomplete status', function () {
            $parser = new BinaryResultSetParser();

            expect($parser->isComplete())->toBeFalse();
        });

        it('can be reset to initial state', function () {
            $parser = new BinaryResultSetParser();
            $parser->reset();

            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('processPayload - column count packet', function () {
        it('processes column count packet correctly', function () {
            $parser = new BinaryResultSetParser();
            $payload = "\x02";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles zero columns', function () {
            $parser = new BinaryResultSetParser();
            $payload = "\x00";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles single column', function () {
            $parser = new BinaryResultSetParser();
            $payload = "\x01";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles large column count with 2-byte encoding', function () {
            $parser = new BinaryResultSetParser();
            $payload = "\xfc\xff\x00";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles medium column count', function () {
            $parser = new BinaryResultSetParser();
            $payload = "\x0a";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('processPayload - EOF packets', function () {
        it('recognizes EOF packet after columns', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");

            $columnPacket = buildMockColumnPacket();
            $parser->processPayload($columnPacket);

            $eofPacket = "\xfe\x00\x00\x02\x00";
            $parser->processPayload($eofPacket);

            expect($parser->isComplete())->toBeFalse();
        });

        it('recognizes EOF packet after rows to complete parsing', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");

            $parser->processPayload(buildMockColumnPacket());

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockRowPacket(1));

            $eofPacket = "\xfe\x00\x00\x02\x00";
            $parser->processPayload($eofPacket);

            expect($parser->isComplete())->toBeTrue();
        });

        it('does not treat long 0xFE packet as EOF', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $longPacket = "\xfe" . str_repeat("\x00", 20);
            $parser->processPayload($longPacket);

            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('processPayload - complete result set flow', function () {
        it('processes complete result set with one column and one row', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");

            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockRowPacket(1));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            expect($result)->toBeInstanceOf(Result::class);
        });

        it('processes complete result set with multiple columns', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x03");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('age', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $columnTypes = [FieldType::LONG, FieldType::VAR_STRING, FieldType::LONG];

            $parser->processPayload(buildMockRowPacket(3, $columnTypes));
            $parser->processPayload(buildMockRowPacket(3, $columnTypes));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes empty result set', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");

            $parser->processPayload(buildMockColumnPacket());

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            expect($result->fetchAllAssoc())->toBeArray()->toBeEmpty();
        });
    });

    describe('processPayload - ignores packets after completion', function () {
        it('ignores payloads after parser is complete', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockRowPacket(1));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());

            expect($parser->isComplete())->toBeTrue();
        });
    });

    describe('reset functionality', function () {
        it('resets parser after completing a result set', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockRowPacket(1));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $parser->reset();

            expect($parser->isComplete())->toBeFalse();

            $parser->processPayload("\x01");
            expect($parser->isComplete())->toBeFalse();
        });

        it('can reset parser in middle of processing', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket());

            $parser->reset();

            expect($parser->isComplete())->toBeFalse();

            $parser->processPayload("\x01");
            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('null bitmap handling', function () {
        it('processes row with no null values', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('V', 42) . "\x04test";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes row with null values in bitmap', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x01" . "\x04test";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles null bitmap for 8 columns', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x08");

            for ($i = 0; $i < 8; $i++) {
                $parser->processPayload(buildMockColumnPacket("col$i", FieldType::LONG));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\xaa" . str_repeat(pack('V', 0), 4);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles null bitmap for 9 columns requiring 2 bytes', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x09");

            for ($i = 0; $i < 9; $i++) {
                $parser->processPayload(buildMockColumnPacket("col$i", FieldType::LONG));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00\x01" . str_repeat(pack('V', 0), 8);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles null bitmap for 16 columns', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x10");

            for ($i = 0; $i < 16; $i++) {
                $parser->processPayload(buildMockColumnPacket("col$i", FieldType::LONG));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\xff\xff" . str_repeat(pack('V', 0), 0);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });
    });

    describe('field type parsing', function () {
        it('processes TINY field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('tiny_col', FieldType::TINY));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('C', 42);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes SHORT field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('short_col', FieldType::SHORT));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('v', 1000);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes LONG field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('long_col', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('V', 100000);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes LONGLONG field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('longlong_col', FieldType::LONGLONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('P', 9999999999);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes FLOAT field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('float_col', FieldType::FLOAT));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('f', 3.14);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes DOUBLE field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('double_col', FieldType::DOUBLE));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('d', 3.141592653589793);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes MEDIUM field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('medium_col', FieldType::MEDIUM));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x39\x30\x00";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes DATE field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('date_col', FieldType::DATE));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x04" . pack('v', 2024) . pack('C', 1) . pack('C', 15);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes DATETIME field type with microseconds', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('datetime_col', FieldType::DATETIME));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x0b"
                . pack('v', 2024) . pack('C', 1) . pack('C', 15)
                . pack('C', 14) . pack('C', 30) . pack('C', 45)
                . pack('V', 123456);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes TIME field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('time_col', FieldType::TIME));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x08"
                . pack('C', 0)
                . pack('V', 0)
                . pack('C', 14) . pack('C', 30) . pack('C', 45);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes TIMESTAMP field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('timestamp_col', FieldType::TIMESTAMP));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x07"
                . pack('v', 2024) . pack('C', 1) . pack('C', 15)
                . pack('C', 14) . pack('C', 30) . pack('C', 45);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('processes default string type for unknown field types', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('string_col', 0xFF));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x05hello";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });
    });

    describe('edge cases', function () {
        it('handles zero-length DATE', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('date_col', FieldType::DATE));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x00";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles zero-length TIME', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('time_col', FieldType::TIME));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x00";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles negative TIME value', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('time_col', FieldType::TIME));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x08"
                . pack('C', 1)
                . pack('V', 2)
                . pack('C', 5) . pack('C', 30) . pack('C', 15);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles TIME with microseconds', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('time_col', FieldType::TIME));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x0c"
                . pack('C', 0)
                . pack('V', 0)
                . pack('C', 14) . pack('C', 30) . pack('C', 45)
                . pack('V', 123456);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles MEDIUM int with negative value', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('medium_col', FieldType::MEDIUM));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\xff\xff\xff";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles multiple rows', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            for ($i = 1; $i <= 5; $i++) {
                $rowPacket = "\x00" . "\x00" . pack('V', $i);
                $parser->processPayload($rowPacket);
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });

        it('handles large number of columns', function () {
            $parser = new BinaryResultSetParser();

            $columnCount = 50;
            $parser->processPayload(pack('C', $columnCount));

            for ($i = 0; $i < $columnCount; $i++) {
                $parser->processPayload(buildMockColumnPacket("col$i", FieldType::LONG));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $nullBitmapSize = (int)(($columnCount + 7) / 8);
            $rowPacket = "\x00" . str_repeat("\x00", $nullBitmapSize) . str_repeat(pack('V', 42), $columnCount);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
        });
    });

    describe('getResult', function () {
        it('returns Result object after completion', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockRowPacket(1));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $result = $parser->getResult();

            expect($result)->toBeInstanceOf(Result::class);
        });

        it('can call getResult before completion', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");

            $result = $parser->getResult();

            expect($result)->toBeInstanceOf(Result::class);
        });
    });

    describe('field type parsing - additional types', function () {
        it('processes YEAR field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('year_col', FieldType::YEAR));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . pack('V', 2024);
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['year_col'])->toBe(2024);
        });

        it('processes DECIMAL field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('decimal_col', FieldType::DECIMAL));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x06123.45";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['decimal_col'])->toBe('123.45');
        });

        it('processes NEWDECIMAL field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('newdecimal_col', FieldType::NEWDECIMAL));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x0899999.99";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['newdecimal_col'])->toBe('99999.99');
        });

        it('processes ENUM field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('enum_col', FieldType::ENUM));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x06active";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['enum_col'])->toBe('active');
        });

        it('processes SET field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('set_col', FieldType::SET));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $setValue = 'read,write';
            $rowPacket = "\x00" . "\x00" . pack('C', strlen($setValue)) . $setValue;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['set_col'])->toBe('read,write');
        });

        it('processes JSON field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('json_col', FieldType::JSON));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $jsonData = '{"name":"test","value":123}';
            $rowPacket = "\x00" . "\x00" . pack('C', strlen($jsonData)) . $jsonData;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['json_col'])->toBe($jsonData);
        });

        it('processes TINY_BLOB field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('tinyblob_col', FieldType::TINY_BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $blobData = "small binary data";
            $rowPacket = "\x00" . "\x00" . pack('C', strlen($blobData)) . $blobData;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['tinyblob_col'])->toBe($blobData);
        });

        it('processes BLOB field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('blob_col', FieldType::BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $blobData = "binary data " . str_repeat("x", 100);
            $rowPacket = "\x00" . "\x00" . pack('C', strlen($blobData)) . $blobData;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['blob_col'])->toBe($blobData);
        });

        it('processes MEDIUM_BLOB field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('mediumblob_col', FieldType::MEDIUM_BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $blobData = str_repeat("data", 50);
            $rowPacket = "\x00" . "\x00" . pack('C', strlen($blobData)) . $blobData;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['mediumblob_col'])->toBe($blobData);
        });

        it('processes LONG_BLOB field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('longblob_col', FieldType::LONG_BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $blobData = str_repeat("large", 100);
            $rowPacket = "\x00" . "\x00" . "\xfc" . pack('v', strlen($blobData)) . $blobData;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['longblob_col'])->toBe($blobData);
        });

        it('processes VAR_STRING field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('varchar_col', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x0bHello World";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['varchar_col'])->toBe('Hello World');
        });

        it('processes STRING field type', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('char_col', FieldType::STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x00" . "\x05FIXED";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['char_col'])->toBe('FIXED');
        });
    });

    describe('field type parsing - mixed types', function () {
        it('processes row with multiple different field types', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x05");

            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('price', FieldType::NEWDECIMAL));
            $parser->processPayload(buildMockColumnPacket('year', FieldType::YEAR));
            $parser->processPayload(buildMockColumnPacket('status', FieldType::ENUM));
            $parser->processPayload(buildMockColumnPacket('data', FieldType::JSON));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $columnTypes = [
                FieldType::LONG,
                FieldType::NEWDECIMAL,
                FieldType::YEAR,
                FieldType::ENUM,
                FieldType::JSON
            ];
            $values = [
                42,
                '99.99',
                2024,
                'active',
                '{"key":"value"}'
            ];

            $parser->processPayload(buildMockRowPacket(5, $columnTypes, $values));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['id'])->toBe(42);
            expect($rows[0]['price'])->toBe('99.99');
            expect($rows[0]['year'])->toBe(2024);
            expect($rows[0]['status'])->toBe('active');
            expect($rows[0]['data'])->toBe('{"key":"value"}');
        });

        it('processes BLOB with binary data containing null bytes', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('binary_col', FieldType::BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $binaryData = "\x00\x01\x02\x03\x00\xff\xfe";
            $rowPacket = "\x00" . "\x00" . pack('C', strlen($binaryData)) . $binaryData;
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();
            expect($rows[0]['binary_col'])->toBe($binaryData);
        });

        it('handles null values for new field types', function () {
            $parser = new BinaryResultSetParser();

            $parser->processPayload("\x04");

            $parser->processPayload(buildMockColumnPacket('decimal_col', FieldType::NEWDECIMAL));
            $parser->processPayload(buildMockColumnPacket('json_col', FieldType::JSON));
            $parser->processPayload(buildMockColumnPacket('enum_col', FieldType::ENUM));
            $parser->processPayload(buildMockColumnPacket('year_col', FieldType::YEAR));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x00" . "\x0F";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['decimal_col'])->toBeNull();
            expect($rows[0]['json_col'])->toBeNull();
            expect($rows[0]['enum_col'])->toBeNull();
            expect($rows[0]['year_col'])->toBeNull();
        });
    });
});
