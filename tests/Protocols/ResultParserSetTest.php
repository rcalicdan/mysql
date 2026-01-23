<?php

use Hibla\MysqlClient\Enums\FieldType;
use Hibla\MysqlClient\Protocols\ResultSetParser;

describe('ResultSetParser', function () {

    describe('initial state', function () {
        it('starts with incomplete status', function () {
            $parser = new ResultSetParser();

            expect($parser->isComplete())->toBeFalse();
        });

        it('can be reset to initial state', function () {
            $parser = new ResultSetParser();
            $parser->reset();

            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('processPayload - column count packet', function () {
        it('processes column count packet correctly', function () {
            $parser = new ResultSetParser();
            $payload = "\x02";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles zero columns', function () {
            $parser = new ResultSetParser();
            $payload = "\x00";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles single column', function () {
            $parser = new ResultSetParser();
            $payload = "\x01";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles large column count with 2-byte encoding', function () {
            $parser = new ResultSetParser();
            $payload = "\xfc\xff\x00";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles medium column count', function () {
            $parser = new ResultSetParser();
            $payload = "\x0a";

            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('processPayload - EOF packets', function () {
        it('recognizes EOF packet after columns', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");

            $columnPacket = buildMockColumnPacket();
            $parser->processPayload($columnPacket);

            $eofPacket = "\xfe\x00\x00\x02\x00";
            $parser->processPayload($eofPacket);

            expect($parser->isComplete())->toBeFalse();
        });

        it('recognizes EOF packet after rows to complete parsing', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");

            $parser->processPayload(buildMockColumnPacket());

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['42']));

            $eofPacket = "\xfe\x00\x00\x02\x00";
            $parser->processPayload($eofPacket);

            expect($parser->isComplete())->toBeTrue();
        });

        it('does not treat long 0xFE packet as EOF', function () {
            $parser = new ResultSetParser();

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
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");

            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['42']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            expect($result)->toBeInstanceOf(\Hibla\MysqlClient\ValueObjects\Result::class);

            $rows = $result->fetchAllAssoc();
            expect($rows)->toHaveCount(1);
            expect($rows[0]['id'])->toBe('42');
        });

        it('processes complete result set with multiple columns', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x03");

            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('age', FieldType::LONG));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['1', 'Alice', '25']));
            $parser->processPayload(buildMockTextRowPacket(['2', 'Bob', '30']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows)->toHaveCount(2);
            expect($rows[0]['id'])->toBe('1');
            expect($rows[0]['name'])->toBe('Alice');
            expect($rows[0]['age'])->toBe('25');
            expect($rows[1]['id'])->toBe('2');
            expect($rows[1]['name'])->toBe('Bob');
            expect($rows[1]['age'])->toBe('30');
        });

        it('processes empty result set', function () {
            $parser = new ResultSetParser();

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
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockTextRowPacket(['42']));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());

            expect($parser->isComplete())->toBeTrue();
        });
    });

    describe('reset functionality', function () {
        it('resets parser after completing a result set', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket());
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockTextRowPacket(['42']));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $parser->reset();

            expect($parser->isComplete())->toBeFalse();

            $parser->processPayload("\x01");
            expect($parser->isComplete())->toBeFalse();
        });

        it('can reset parser in middle of processing', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket());

            $parser->reset();

            expect($parser->isComplete())->toBeFalse();

            $parser->processPayload("\x01");
            expect($parser->isComplete())->toBeFalse();
        });
    });

    describe('text protocol specifics', function () {
        it('handles NULL values as null bytes in text protocol', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowPacket = "\x0242\xfb";
            $parser->processPayload($rowPacket);

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['id'])->toBe('42');
            expect($rows[0]['name'])->toBeNull();
        });

        it('handles multiple rows with varying data', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('email', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['1', 'alice@example.com']));
            $parser->processPayload(buildMockTextRowPacket(['2', 'bob@example.com']));
            $parser->processPayload(buildMockTextRowPacket(['3', 'charlie@example.com']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows)->toHaveCount(3);
            expect($rows[0]['email'])->toBe('alice@example.com');
            expect($rows[1]['email'])->toBe('bob@example.com');
            expect($rows[2]['email'])->toBe('charlie@example.com');
        });

        it('handles numeric strings correctly', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x03");
            $parser->processPayload(buildMockColumnPacket('integer', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('decimal', FieldType::NEWDECIMAL));
            $parser->processPayload(buildMockColumnPacket('float', FieldType::FLOAT));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['42', '99.99', '3.14159']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['integer'])->toBe('42');
            expect($rows[0]['decimal'])->toBe('99.99');
            expect($rows[0]['float'])->toBe('3.14159');
        });

        it('handles empty strings', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('value', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['value'])->toBe('');
        });

        it('handles long text values', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('description', FieldType::BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $longText = str_repeat('Lorem ipsum dolor sit amet. ', 100);
            $parser->processPayload(buildMockTextRowPacket([$longText]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['description'])->toBe($longText);
        });
    });

    describe('getResult', function () {
        it('returns Result object after completion', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockTextRowPacket(['1']));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $result = $parser->getResult();

            expect($result)->toBeInstanceOf(\Hibla\MysqlClient\ValueObjects\Result::class);
        });

        it('can call getResult before completion', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");

            $result = $parser->getResult();

            expect($result)->toBeInstanceOf(\Hibla\MysqlClient\ValueObjects\Result::class);
        });
    });

    describe('edge cases', function () {
        it('handles large number of columns', function () {
            $parser = new ResultSetParser();

            $columnCount = 50;
            $parser->processPayload(pack('C', $columnCount));

            for ($i = 0; $i < $columnCount; $i++) {
                $parser->processPayload(buildMockColumnPacket("col$i", FieldType::VAR_STRING));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $values = array_map(fn($i) => "value$i", range(0, $columnCount - 1));
            $parser->processPayload(buildMockTextRowPacket($values));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0])->toHaveCount($columnCount);
        });

        it('handles special characters in strings', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('data', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $specialChars = "Hello\nWorld\t\"Quotes\"'Single'\x00Null";
            $parser->processPayload(buildMockTextRowPacket([$specialChars]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['data'])->toBe($specialChars);
        });
    });

    describe('edge cases - additional coverage', function () {
        it('handles very large column count with 3-byte length encoding', function () {
            $parser = new ResultSetParser();

            $columnCount = 70000;
            $payload = "\xfd" . substr(pack('V', $columnCount), 0, 3);
            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles column count of exactly 251 (boundary case)', function () {
            $parser = new ResultSetParser();

            $payload = "\xfc\xfb\x00";
            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles column count of 250 (just below boundary)', function () {
            $parser = new ResultSetParser();

            $payload = "\xfa";
            $parser->processPayload($payload);

            expect($parser->isComplete())->toBeFalse();
        });

        it('handles UTF-8 multibyte characters', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $utf8Text = "Hello 世界 🌍 مرحبا Привет";
            $parser->processPayload(buildMockTextRowPacket([$utf8Text]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['name'])->toBe($utf8Text);
        });

        it('handles binary data in BLOB field', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('binary_data', FieldType::BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $binaryData = "\x00\x01\x02\x03\xff\xfe\xfd\x00\x00\x10\x20\x30";
            $parser->processPayload(buildMockTextRowPacket([$binaryData]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['binary_data'])->toBe($binaryData);
        });

        it('handles extremely long string (255+ bytes, 2-byte length)', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('long_text', FieldType::BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $longString = str_repeat('A', 300);
            $parser->processPayload(buildMockTextRowPacket([$longString]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['long_text'])->toBe($longString);
            expect(strlen($rows[0]['long_text']))->toBe(300);
        });

        it('handles extremely long string (65536+ bytes, 3-byte length)', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('huge_text', FieldType::LONG_BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $hugeString = str_repeat('B', 70000);
            $parser->processPayload(buildMockTextRowPacket([$hugeString]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect(strlen($rows[0]['huge_text']))->toBe(70000);
        });

        it('handles all NULL values in a row', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x03");
            $parser->processPayload(buildMockColumnPacket('col1', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('col2', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('col3', FieldType::DOUBLE));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket([null, null, null]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['col1'])->toBeNull();
            expect($rows[0]['col2'])->toBeNull();
            expect($rows[0]['col3'])->toBeNull();
        });

        it('handles mixed NULL and non-NULL values', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x04");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('email', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('phone', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['1', 'Alice', null, '555-1234']));
            $parser->processPayload(buildMockTextRowPacket(['2', null, 'bob@example.com', null]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['id'])->toBe('1');
            expect($rows[0]['name'])->toBe('Alice');
            expect($rows[0]['email'])->toBeNull();
            expect($rows[0]['phone'])->toBe('555-1234');

            expect($rows[1]['id'])->toBe('2');
            expect($rows[1]['name'])->toBeNull();
            expect($rows[1]['email'])->toBe('bob@example.com');
            expect($rows[1]['phone'])->toBeNull();
        });

        it('handles very large number of rows', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('counter', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $rowCount = 1000;
            for ($i = 1; $i <= $rowCount; $i++) {
                $parser->processPayload(buildMockTextRowPacket([(string)$i]));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows)->toHaveCount($rowCount);
            expect($rows[0]['counter'])->toBe('1');
            expect($rows[999]['counter'])->toBe('1000');
        });

        it('handles string with only whitespace', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('text', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['   ']));
            $parser->processPayload(buildMockTextRowPacket(["\t\t\t"]));
            $parser->processPayload(buildMockTextRowPacket(["\n\n"]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['text'])->toBe('   ');
            expect($rows[1]['text'])->toBe("\t\t\t");
            expect($rows[2]['text'])->toBe("\n\n");
        });

        it('handles zero-width string (empty but not NULL)', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket('empty', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('null_val', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['', null]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['empty'])->toBe('');
            expect($rows[0]['null_val'])->toBeNull();
        });

        it('handles numeric edge cases as strings', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x05");
            $parser->processPayload(buildMockColumnPacket('zero', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('negative', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('large', FieldType::LONGLONG));
            $parser->processPayload(buildMockColumnPacket('decimal', FieldType::NEWDECIMAL));
            $parser->processPayload(buildMockColumnPacket('scientific', FieldType::DOUBLE));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket([
                '0',
                '-42',
                '9223372036854775807',
                '999999.999999',
                '1.23e-10'
            ]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['zero'])->toBe('0');
            expect($rows[0]['negative'])->toBe('-42');
            expect($rows[0]['large'])->toBe('9223372036854775807');
            expect($rows[0]['decimal'])->toBe('999999.999999');
            expect($rows[0]['scientific'])->toBe('1.23e-10');
        });

        it('handles date/time as strings in text protocol', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x04");
            $parser->processPayload(buildMockColumnPacket('date_col', FieldType::DATE));
            $parser->processPayload(buildMockColumnPacket('time_col', FieldType::TIME));
            $parser->processPayload(buildMockColumnPacket('datetime_col', FieldType::DATETIME));
            $parser->processPayload(buildMockColumnPacket('timestamp_col', FieldType::TIMESTAMP));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket([
                '2024-01-15',
                '14:30:45',
                '2024-01-15 14:30:45',
                '2024-01-15 14:30:45'
            ]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['date_col'])->toBe('2024-01-15');
            expect($rows[0]['time_col'])->toBe('14:30:45');
            expect($rows[0]['datetime_col'])->toBe('2024-01-15 14:30:45');
            expect($rows[0]['timestamp_col'])->toBe('2024-01-15 14:30:45');
        });

        it('handles JSON as string in text protocol', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('json_col', FieldType::JSON));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $jsonString = '{"name":"Alice","age":30,"tags":["developer","mysql"]}';
            $parser->processPayload(buildMockTextRowPacket([$jsonString]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['json_col'])->toBe($jsonString);
            expect(json_decode($rows[0]['json_col'], true))->toBeArray();
        });

        it('handles column names with special characters', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x03");
            $parser->processPayload(buildMockColumnPacket('user_id', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('first-name', FieldType::VAR_STRING));
            $parser->processPayload(buildMockColumnPacket('email@domain', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['1', 'Alice', 'alice@example.com']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['user_id'])->toBe('1');
            expect($rows[0]['first-name'])->toBe('Alice');
            expect($rows[0]['email@domain'])->toBe('alice@example.com');
        });

        it('handles SQL injection attempt strings safely', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('malicious', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $maliciousStrings = [
                "'; DROP TABLE users; --",
                "1' OR '1'='1",
                "admin'--",
                "' UNION SELECT * FROM passwords--"
            ];

            foreach ($maliciousStrings as $str) {
                $parser->processPayload(buildMockTextRowPacket([$str]));
            }

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows)->toHaveCount(4);
            expect($rows[0]['malicious'])->toBe("'; DROP TABLE users; --");
        });

        it('handles backslash and escape sequences', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('path', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $parser->processPayload(buildMockTextRowPacket(['C:\\Users\\Alice\\Documents']));
            $parser->processPayload(buildMockTextRowPacket(['\n\t\r\\']));
            $parser->processPayload(buildMockTextRowPacket(['\\\\network\\share']));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['path'])->toBe('C:\\Users\\Alice\\Documents');
            expect($rows[1]['path'])->toBe('\n\t\r\\');
            expect($rows[2]['path'])->toBe('\\\\network\\share');
        });

        it('handles consecutive EOF markers in row data', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('data', FieldType::BLOB));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $dataWithFE = "\xfe\xfe\xfe" . str_repeat("x", 20);
            $parser->processPayload(buildMockTextRowPacket([$dataWithFE]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect($rows[0]['data'])->toContain("\xfe\xfe\xfe");
        });

        it('handles row with single column value of exactly 250 bytes', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('boundary', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            $boundaryString = str_repeat('X', 250);
            $parser->processPayload(buildMockTextRowPacket([$boundaryString]));

            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();

            $result = $parser->getResult();
            $rows = $result->fetchAllAssoc();

            expect(strlen($rows[0]['boundary']))->toBe(250);
        });

        it('handles multiple resets and reuses', function () {
            $parser = new ResultSetParser();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('id', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockTextRowPacket(['1']));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
            $result1 = $parser->getResult();

            $parser->reset();

            $parser->processPayload("\x01");
            $parser->processPayload(buildMockColumnPacket('name', FieldType::VAR_STRING));
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockTextRowPacket(['Alice']));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
            $result2 = $parser->getResult();

            $parser->reset();

            $parser->processPayload("\x02");
            $parser->processPayload(buildMockColumnPacket('x', FieldType::LONG));
            $parser->processPayload(buildMockColumnPacket('y', FieldType::LONG));
            $parser->processPayload("\xfe\x00\x00\x02\x00");
            $parser->processPayload(buildMockTextRowPacket(['10', '20']));
            $parser->processPayload("\xfe\x00\x00\x02\x00");

            expect($parser->isComplete())->toBeTrue();
            $result3 = $parser->getResult();

            expect($result1->fetchAllAssoc()[0])->toHaveKey('id');
            expect($result2->fetchAllAssoc()[0])->toHaveKey('name');
            expect($result3->fetchAllAssoc()[0])->toHaveKeys(['x', 'y']);
        });
    });
});
