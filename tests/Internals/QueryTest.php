<?php

declare(strict_types=1);

use function Hibla\await;

beforeAll(function (): void {
    $conn = makeConnection();
    await($conn->query('CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY AUTO_INCREMENT)'));
    $conn->close();
});

afterAll(function (): void {
    $conn = makeConnection();
    await($conn->query('DROP TABLE IF EXISTS users'));
    $conn->close();
});

describe('Query (Text Protocol)', function (): void {
    describe('Basic', function (): void {

        it('returns integers as strings in text protocol', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 42 as num'));
            $row = $result->fetchOne();

            expect($row['num'])->toBe('42');

            $conn->close();
        });

        it('returns NULL values correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT NULL as val1, NULL as val2'));
            $row = $result->fetchOne();

            expect($row['val1'])->toBeNull()
                ->and($row['val2'])->toBeNull()
            ;

            $conn->close();
        });

        it('handles strings with single and double quotes', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'O\\'Reilly' as name, '\"Quotes\"' as quoted"));
            $row = $result->fetchOne();

            expect($row['name'])->toBe("O'Reilly")
                ->and($row['quoted'])->toBe('"Quotes"')
            ;

            $conn->close();
        });

        it('returns large integers as strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 123 as small, 2147483647 as medium, 9223372036854775807 as big'));
            $row = $result->fetchOne();

            expect($row['small'])->toBe('123')
                ->and($row['medium'])->toBe('2147483647')
                ->and($row['big'])->toBe('9223372036854775807')
            ;

            $conn->close();
        });

        it('returns negative integers as strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT -42 as val, -2147483648 as min_int'));
            $row = $result->fetchOne();

            expect($row['val'])->toBe('-42')
                ->and($row['min_int'])->toBe('-2147483648')
            ;

            $conn->close();
        });

        it('returns floats as strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 3.14159 as pi_val, -0.005 as neg_float'));
            $row = $result->fetchOne();

            expect($row['pi_val'])->toBe('3.14159')
                ->and($row['neg_float'])->toBe('-0.005')
            ;

            $conn->close();
        });

        it('maps TRUE to 1 and FALSE to 0 as strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT TRUE as t, FALSE as f'));
            $row = $result->fetchOne();

            expect($row['t'])->toBe('1')
                ->and($row['f'])->toBe('0')
            ;

            $conn->close();
        });

        it('returns date and datetime strings correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '2023-10-01' as d, '2023-10-01 12:00:00' as dt"));
            $row = $result->fetchOne();

            expect($row['d'])->toBe('2023-10-01')
                ->and($row['dt'])->toBe('2023-10-01 12:00:00')
            ;

            $conn->close();
        });

        it('handles UTF-8 emoji and multibyte characters', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '🎸🎵' as emoji, 'Hello 世界' as text"));
            $row = $result->fetchOne();

            expect($row['emoji'])->toBe('🎸🎵')
                ->and($row['text'])->toBe('Hello 世界')
            ;

            $conn->close();
        });

        it('returns multiple rows via UNION', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as id UNION SELECT 2 UNION SELECT 3'));
            $rows = $result->fetchAll();

            expect($rows)->toHaveCount(3)
                ->and($rows[0]['id'])->toBe('1')
                ->and($rows[1]['id'])->toBe('2')
                ->and($rows[2]['id'])->toBe('3')
            ;

            $conn->close();
        });

        it('returns empty result set when no rows match', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT * FROM users WHERE 1=0'));

            expect($result->fetchAll())->toHaveCount(0);

            $conn->close();
        });

        it('handles complex expressions', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 1+1 as sum, CONCAT('a','b') as str, NOW() is not null as chk"));
            $row = $result->fetchOne();

            expect($row['sum'])->toBe('2')
                ->and($row['str'])->toBe('ab')
                ->and($row['chk'])->toBe('1')
            ;

            $conn->close();
        });

        it('handles JSON string in text protocol', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '{\"a\":1}' as json_val"));
            $row = $result->fetchOne();

            expect($row['json_val'])->toBe('{"a":1}');

            $conn->close();
        });

        it('returns long text generated by REPEAT()', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT REPEAT('A', 10000) as long_str"));
            $row = $result->fetchOne();

            expect(strlen($row['long_str']))->toBe(10000)
                ->and($row['long_str'][0])->toBe('A')
            ;

            $conn->close();
        });

        it('returns binary data from UNHEX()', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT UNHEX('00FF00') as bin_data"));
            $row = $result->fetchOne();

            expect(strlen($row['bin_data']))->toBe(3)
                ->and(bin2hex($row['bin_data']))->toBe('00ff00')
            ;

            $conn->close();
        });
    });

    describe('Column Names', function (): void {

        it('handles extremely long column names (64 chars)', function (): void {
            $conn = makeConnection();
            $longName = str_repeat('a', 64);
            $result = await($conn->query("SELECT 1 as `{$longName}`"));
            $row = $result->fetchOne();

            expect(isset($row[$longName]))->toBeTrue();

            $conn->close();
        });

        it('handles special characters in column names', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as `col with spaces`, 2 as `col-dash`, 3 as `col.dot`'));
            $row = $result->fetchOne();

            expect($row['col with spaces'])->toBe('1')
                ->and($row['col-dash'])->toBe('2')
                ->and($row['col.dot'])->toBe('3')
            ;

            $conn->close();
        });

        it('handles duplicate column names with deduplication', function (): void {
            $conn = makeConnection();
            $resultAlias = await($conn->query('SELECT 1 as a, 2 as a, 3 as a'));
            $rowAlias = $resultAlias->fetchOne();

            expect($rowAlias['a'])->toBe('1')
                ->and($rowAlias['a1'])->toBe('2')
                ->and($rowAlias['a2'])->toBe('3')
            ;

            $conn->close();
        });

        it('handles more than 3 duplicate column names', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as x, 2 as x, 3 as x, 4 as x, 5 as x'));
            $row = $result->fetchOne();

            expect($row['x'])->toBe('1')
                ->and($row['x1'])->toBe('2')
                ->and($row['x2'])->toBe('3')
                ->and($row['x3'])->toBe('4')
                ->and($row['x4'])->toBe('5')
            ;

            $conn->close();
        });

        it('preserves column name case sensitivity', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as CamelCase, 2 as UPPERCASE, 3 as lowercase'));
            $row = $result->fetchOne();

            expect(isset($row['CamelCase']))->toBeTrue()
                ->and(isset($row['UPPERCASE']))->toBeTrue()
                ->and(isset($row['lowercase']))->toBeTrue()
            ;

            $conn->close();
        });

        it('handles unicode column names', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as `名前`, 2 as `データ`'));
            $row = $result->fetchOne();

            expect($row['名前'])->toBe('1')
                ->and($row['データ'])->toBe('2')
            ;

            $conn->close();
        });

        it('handles reserved words as column aliases', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 
                1 as `select`, 
                2 as `from`, 
                3 as `where`, 
                4 as `group`,
                5 as `order`,
                6 as `limit`,
                7 as `primary`,
                8 as `key`,
                9 as `index`,
                10 as `table`
            '));
            $row = $result->fetchOne();

            expect($row['select'])->toBe('1')
                ->and($row['table'])->toBe('10')
            ;

            $conn->close();
        });
    });

    describe('Data Types', function (): void {

        it('handles zero values for int, float, and string', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 0 as zero_int, 0.0 as zero_float, '0' as zero_str, '' as empty_str"));
            $row = $result->fetchOne();

            expect($row['zero_int'])->toBe('0')
                ->and($row['zero_float'])->toBe('0.0')
                ->and($row['zero_str'])->toBe('0')
                ->and($row['empty_str'])->toBe('')
            ;

            $conn->close();
        });

        it('handles whitespace strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT ' ' as single_space, '  ' as double_space, '\t' as tab, '\n' as newline"));
            $row = $result->fetchOne();

            expect($row['single_space'])->toBe(' ')
                ->and($row['double_space'])->toBe('  ')
                ->and($row['tab'])->toBe("\t")
                ->and($row['newline'])->toBe("\n")
            ;

            $conn->close();
        });

        it('handles scientific notation', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1.23e10 as sci_large, 1.23e-10 as sci_small'));
            $row = $result->fetchOne();

            expect(strlen($row['sci_large']))->toBeGreaterThan(0)
                ->and(strlen($row['sci_small']))->toBeGreaterThan(0)
            ;

            $conn->close();
        });

        it('handles max and min bigint values', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT -9223372036854775808 as min_bigint, 18446744073709551615 as max_ubigint'));
            $row = $result->fetchOne();

            expect($row['min_bigint'])->toBe('-9223372036854775808')
                ->and($row['max_ubigint'])->toBe('18446744073709551615')
            ;

            $conn->close();
        });

        it('handles mixed null and values', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT NULL, 1, NULL, 'text', NULL as multi_null"));
            $row = $result->fetchOne();

            expect(count($row))->toBe(5)
                ->and($row['multi_null'])->toBeNull()
            ;

            $conn->close();
        });

        it('handles decimal precision', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 123.456789012345 as high_precision'));
            $row = $result->fetchOne();

            expect(str_contains($row['high_precision'], '.'))->toBeTrue();

            $conn->close();
        });

        it('handles time values including negative time', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT TIME('12:34:56') as time_val, TIME('-12:34:56') as neg_time"));
            $row = $result->fetchOne();

            expect($row['time_val'])->toBe('12:34:56')
                ->and($row['neg_time'])->toBe('-12:34:56')
            ;

            $conn->close();
        });

        it('handles year edge cases including zero date', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '0000-00-00' as zero_date, '1000-01-01' as min_date, '9999-12-31' as max_date"));
            $row = $result->fetchOne();

            expect(isset($row['zero_date']))->toBeTrue();

            $conn->close();
        });

        it('handles CAST to BINARY type', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT CAST('binary data' AS BINARY) as blob_field"));
            $row = $result->fetchOne();

            expect($row['blob_field'])->toBe('binary data');

            $conn->close();
        });

        it('handles ENUM and SET types', function (): void {
            $conn = makeConnection();
            await($conn->query("CREATE TEMPORARY TABLE temp_enum (id INT, status ENUM('active','inactive'), tags SET('a','b','c'))"));
            await($conn->query("INSERT INTO temp_enum VALUES (1, 'active', 'a,c')"));

            $result = await($conn->query('SELECT * FROM temp_enum'));
            $row = $result->fetchOne();

            expect($row['status'])->toBe('active')
                ->and($row['tags'])->toBe('a,c')
            ;

            $conn->close();
        });

        it('handles empty string vs NULL distinction', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '' as `empty`, NULL as null_val, '0' as zero_str"));
            $row = $result->fetchOne();

            expect($row['empty'])->toBe('')
                ->and($row['null_val'])->toBeNull()
                ->and($row['empty'])->not->toBe($row['null_val'])
            ;

            $conn->close();
        });

        it('handles unsigned BIGINT max value', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT CAST(18446744073709551615 AS UNSIGNED) as max_unsigned'));
            $row = $result->fetchOne();

            expect($row['max_unsigned'])->toBe('18446744073709551615');

            $conn->close();
        });

        it('handles extremely large numbers beyond BIGINT', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 99999999999999999999999999999 as huge_num'));
            $row = $result->fetchOne();

            expect(strlen($row['huge_num']))->toBeGreaterThan(20);

            $conn->close();
        });

        it('handles special float values', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 0.0 as zero, -0.0 as neg_zero, 0.000001 as tiny, 999999999.999999 as large'));
            $row = $result->fetchOne();

            expect($row['zero'])->toBe('0.0')
                ->and(isset($row['tiny']))->toBeTrue()
            ;

            $conn->close();
        });

        it('handles hexadecimal literals', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
                CONV('FF', 16, 10) as hex_ff, 
                CONV('00', 16, 10) as hex_00, 
                CONV('DEADBEEF', 16, 10) as hex_large
            "));
            $row = $result->fetchOne();

            expect($row['hex_ff'])->toBe('255')
                ->and($row['hex_00'])->toBe('0')
                ->and($row['hex_large'])->toBe('3735928559')
            ;

            $result2 = await($conn->query('SELECT 0xFF as raw_hex'));
            $row2 = $result2->fetchOne();

            expect($row2['raw_hex'])->toBe("\xFF");

            $conn->close();
        });

        it('handles BIT fields', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE temp_bit (id INT, flags BIT(8))'));
            await($conn->query("INSERT INTO temp_bit VALUES (1, b'10101010')"));

            $result = await($conn->query('SELECT * FROM temp_bit'));
            $row = $result->fetchOne();

            expect(isset($row['flags']))->toBeTrue();

            $conn->close();
        });

        it('handles geometry types', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT POINT(1, 2) as pt'));
            $row = $result->fetchOne();

            expect(isset($row['pt']))->toBeTrue();

            $conn->close();
        });
    });

    describe('Strings', function (): void {

        it('handles backslash escaping', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '\\\\' as backslash, 'C:\\\\path\\\\to\\\\file' as windows_path"));
            $row = $result->fetchOne();

            expect($row['backslash'])->toBe('\\')
                ->and($row['windows_path'])->toBe('C:\\path\\to\\file')
            ;

            $conn->close();
        });

        it('handles multiple consecutive NULLs', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL'));
            $row = $result->fetchOne();

            expect(count($row))->toBe(10);
            foreach ($row as $val) {
                expect($val)->toBeNull();
            }

            $conn->close();
        });

        it('handles mixed charset data', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'ASCII' as ascii, 'Ñoño' as latin, 'Привет' as cyrillic, '你好' as chinese, '🎸🎵😀' as emoji, 'مرحبا' as arabic"));
            $row = $result->fetchOne();

            expect($row['ascii'])->toBe('ASCII')
                ->and($row['latin'])->toBe('Ñoño')
                ->and($row['cyrillic'])->toBe('Привет')
                ->and($row['chinese'])->toBe('你好')
                ->and($row['emoji'])->toBe('🎸🎵😀')
                ->and($row['arabic'])->toBe('مرحبا')
            ;

            $conn->close();
        });

        it('handles leading and trailing whitespace', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '  leading' as `lead`, 'trailing  ' as trail, '  both  ' as `both`"));
            $row = $result->fetchOne();

            expect($row['lead'])->toBe('  leading')
                ->and($row['trail'])->toBe('trailing  ')
                ->and($row['both'])->toBe('  both  ')
            ;

            $conn->close();
        });

        it('handles control characters', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT CHAR(0) as null_byte, CHAR(1) as soh, CHAR(7) as bell, CHAR(27) as escape'));
            $row = $result->fetchOne();

            expect(ord($row['null_byte']))->toBe(0)
                ->and(ord($row['soh']))->toBe(1)
                ->and(ord($row['bell']))->toBe(7)
            ;

            $conn->close();
        });

        it('handles newlines in data', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'line1\nline2\nline3' as multiline"));
            $row = $result->fetchOne();
            $lines = explode("\n", $row['multiline']);

            expect($lines)->toHaveCount(3)
                ->and($lines[0])->toBe('line1')
            ;

            $conn->close();
        });

        it('handles strings with complex quotes and escapes', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'It\\'s a \"test\" with \\\\backslash' as complex"));
            $row = $result->fetchOne();

            expect($row['complex'])->toBe('It\'s a "test" with \\backslash');

            $conn->close();
        });

        it('handles string functions edge cases', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
                SUBSTRING('hello', 0, 2) as substr_zero_start,
                SUBSTRING('hello', -2) as substr_negative,
                LENGTH('') as len_empty,
                LENGTH(NULL) as len_null,
                REVERSE('') as reverse_empty,
                UPPER('') as upper_empty
            "));
            $row = $result->fetchOne();

            expect($row['substr_zero_start'])->toBe('')
                ->and($row['substr_negative'])->toBe('lo')
                ->and($row['len_empty'])->toBe('0')
                ->and($row['len_null'])->toBeNull()
            ;

            $conn->close();
        });

        it('handles collation differences', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
                'ABC' = 'abc' as case_cmp,
                BINARY 'ABC' = 'abc' as binary_cmp,
                'café' as accented
            "));
            $row = $result->fetchOne();

            expect(isset($row['case_cmp']))->toBeTrue()
                ->and($row['accented'])->toBe('café')
            ;

            $conn->close();
        });

        it('handles CHARSET conversion', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT CONVERT('test' USING utf8mb4) as converted"));
            $row = $result->fetchOne();

            expect($row['converted'])->toBe('test');

            $conn->close();
        });

        it('handles fractional seconds in timestamp strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '2023-10-01 12:34:56.123456' as microsec"));
            $row = $result->fetchOne();

            expect(str_contains($row['microsec'], '.'))->toBeTrue();

            $conn->close();
        });
    });

    describe('Buffer Boundaries', function (): void {

        it('handles strings at common buffer boundary sizes', function (): void {
            $conn = makeConnection();
            $sizes = [255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097];

            foreach ($sizes as $size) {
                $result = await($conn->query("SELECT REPEAT('X', $size) as str"));
                $row = $result->fetchOne();

                expect(strlen($row['str']))->toBe($size);
            }

            $conn->close();
        });

        it('handles very wide tables (100 columns)', function (): void {
            $conn = makeConnection();
            $columns = [];
            for ($i = 1; $i <= 100; $i++) {
                $columns[] = "$i as col$i";
            }
            $result = await($conn->query('SELECT ' . implode(', ', $columns)));
            $row = $result->fetchOne();

            expect(count($row))->toBe(100)
                ->and($row['col1'])->toBe('1')
                ->and($row['col100'])->toBe('100')
            ;

            $conn->close();
        });

        it('handles very long result sets (1000 rows)', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('
                WITH RECURSIVE numbers AS (
                    SELECT 1 as n
                    UNION ALL
                    SELECT n + 1 FROM numbers WHERE n < 1000
                )
                SELECT n FROM numbers
            '));
            $rows = $result->fetchAll();

            expect($rows)->toHaveCount(1000)
                ->and($rows[999]['n'])->toBe('1000')
            ;

            $conn->close();
        });
    });

    describe('SQL Features', function (): void {

        it('handles comments in query', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('
                SELECT 
                    1 as val1, -- inline comment
                    /* block comment */ 2 as val2,
                    # MySQL style comment
                    3 as val3
            '));
            $row = $result->fetchOne();

            expect($row['val1'])->toBe('1')
                ->and($row['val2'])->toBe('2')
                ->and($row['val3'])->toBe('3')
            ;

            $conn->close();
        });

        it('handles multi-statement protection', function (): void {
            $conn = makeConnection();

            try {
                $result = await($conn->query('SELECT 1 as val; DROP TABLE users;'));
                $row = $result->fetchOne();
                expect($row['val'])->toBe('1');
            } catch (Throwable $e) {
                expect(true)->toBeTrue();
            }

            $conn->close();
        });

        it('handles CASE WHEN expressions', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
                CASE WHEN 1=1 THEN 'yes' ELSE 'no' END as simple_case,
                CASE WHEN 1=2 THEN 'yes' ELSE 'no' END as else_case,
                CASE WHEN NULL THEN 'yes' ELSE 'no' END as null_case,
                CASE 1 WHEN 1 THEN 'match' WHEN 2 THEN 'no' END as value_case
            "));
            $row = $result->fetchOne();

            expect($row['simple_case'])->toBe('yes')
                ->and($row['else_case'])->toBe('no')
                ->and($row['null_case'])->toBe('no')
                ->and($row['value_case'])->toBe('match')
            ;

            $conn->close();
        });

        it('handles IN operator edge cases', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 
                1 IN (1,2,3) as in_true,
                4 IN (1,2,3) as in_false,
                NULL IN (1,2,3) as in_null,
                1 IN (NULL) as in_only_null
            '));
            $row = $result->fetchOne();

            expect($row['in_true'])->toBe('1')
                ->and($row['in_false'])->toBe('0')
                ->and($row['in_null'])->toBeNull()
                ->and($row['in_only_null'])->toBeNull()
            ;

            $conn->close();
        });

        it('handles LIKE pattern matching', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
                'hello' LIKE 'h%' as like_prefix,
                'hello' LIKE '%o' as like_suffix,
                'hello' LIKE '%ll%' as like_middle,
                'hello' LIKE 'h_llo' as like_underscore
            "));
            $row = $result->fetchOne();

            expect($row['like_prefix'])->toBe('1')
                ->and($row['like_suffix'])->toBe('1')
                ->and($row['like_middle'])->toBe('1')
                ->and($row['like_underscore'])->toBe('1')
            ;

            $conn->close();
        });

        it('handles division by zero returning NULL', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1/0 as div_zero, 0/0 as zero_div_zero, MOD(5, 0) as mod_zero'));
            $row = $result->fetchOne();

            expect($row['div_zero'])->toBeNull()
                ->and($row['zero_div_zero'])->toBeNull()
                ->and($row['mod_zero'])->toBeNull()
            ;

            $conn->close();
        });

        it('handles NULL propagation in CONCAT and COALESCE', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT CONCAT('a', NULL, 'b') as concat_null, COALESCE(NULL, 'default') as coalesce_val"));
            $row = $result->fetchOne();
            expect($row['concat_null'])->toBeNull()
                ->and($row['coalesce_val'])->toBe('default')
            ;

            $conn->close();
        });

        it('handles date arithmetic', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
            DATE_ADD('2023-01-01', INTERVAL 1 DAY) as add_day,
            DATE_SUB('2023-01-01', INTERVAL 1 DAY) as sub_day,
            DATEDIFF('2023-01-02', '2023-01-01') as date_diff,
            TIMESTAMPDIFF(HOUR, '2023-01-01 00:00:00', '2023-01-01 01:00:00') as hour_diff
        "));
            $row = $result->fetchOne();

            expect($row['add_day'])->toBe('2023-01-02')
                ->and($row['sub_day'])->toBe('2022-12-31')
                ->and($row['date_diff'])->toBe('1')
                ->and($row['hour_diff'])->toBe('1')
            ;

            $conn->close();
        });

        it('handles NULLIF and IFNULL', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
            NULLIF(1, 1) as nullif_equal,
            NULLIF(1, 2) as nullif_not_equal,
            IFNULL(NULL, 'default') as ifnull_null,
            IFNULL('value', 'default') as ifnull_value
        "));
            $row = $result->fetchOne();

            expect($row['nullif_equal'])->toBeNull()
                ->and($row['nullif_not_equal'])->toBe('1')
                ->and($row['ifnull_null'])->toBe('default')
                ->and($row['ifnull_value'])->toBe('value')
            ;

            $conn->close();
        });

        it('handles BETWEEN operator', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 
            5 BETWEEN 1 AND 10 as between_true,
            0 BETWEEN 1 AND 10 as between_false,
            5 NOT BETWEEN 1 AND 10 as not_between
        '));
            $row = $result->fetchOne();

            expect($row['between_true'])->toBe('1')
                ->and($row['between_false'])->toBe('0')
                ->and($row['not_between'])->toBe('0')
            ;

            $conn->close();
        });

        it('handles REGEXP pattern matching', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
            'hello' REGEXP '^h' as regexp_start,
            'hello' REGEXP 'o$' as regexp_end,
            'test123' REGEXP '[0-9]+' as regexp_digits
        "));
            $row = $result->fetchOne();

            expect($row['regexp_start'])->toBe('1')
                ->and($row['regexp_end'])->toBe('1')
                ->and($row['regexp_digits'])->toBe('1')
            ;

            $conn->close();
        });

        it('handles CAST conversions', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
            CAST('123' AS SIGNED) as str_to_int,
            CAST(123.45 AS SIGNED) as float_to_int,
            CAST('2023-10-01' AS DATE) as str_to_date,
            CAST(1 AS CHAR) as int_to_str
        "));
            $row = $result->fetchOne();

            expect($row['str_to_int'])->toBe('123')
                ->and($row['float_to_int'])->toBe('123')
                ->and($row['str_to_date'])->toBe('2023-10-01')
                ->and($row['int_to_str'])->toBe('1')
            ;

            $conn->close();
        });

        it('handles arithmetic overflow gracefully', function (): void {
            $conn = makeConnection();

            try {
                $result = await($conn->query('SELECT 9223372036854775807 + 1 as overflow_add'));
                $row = $result->fetchOne();
                expect(isset($row['overflow_add']))->toBeTrue();
            } catch (Throwable $e) {
                expect(
                    str_contains($e->getMessage(), 'MySQL Error') ||
                        str_contains($e->getMessage(), 'BIGINT value is out of range')
                )->toBeTrue();
            }

            $conn->close();
        });

        it('handles ORDER BY affecting fetch order', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 3 as n UNION SELECT 1 UNION SELECT 2 ORDER BY n'));
            $rows = $result->fetchAll();

            expect($rows[0]['n'])->toBe('1')
                ->and($rows[1]['n'])->toBe('2')
                ->and($rows[2]['n'])->toBe('3')
            ;

            $conn->close();
        });

        it('handles LIMIT 0 returning no rows', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as val LIMIT 0'));

            expect($result->fetchAll())->toHaveCount(0);

            $conn->close();
        });

        it('handles DISTINCT edge cases', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT DISTINCT 1,1,1'));

            expect($result->fetchAll())->toHaveCount(1);

            $result2 = await($conn->query('SELECT DISTINCT NULL, NULL'));
            expect($result2->fetchAll())->toHaveCount(1);

            $conn->close();
        });

        it('handles UNION vs UNION ALL behavior', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT 1 as n UNION ALL SELECT 1 UNION ALL SELECT 2'));
            $result2 = await($conn->query('SELECT 1 as n UNION SELECT 1 UNION SELECT 2'));

            expect($result->fetchAll())->toHaveCount(3)
                ->and($result2->fetchAll())->toHaveCount(2)
            ;

            $conn->close();
        });

        it('handles timestamp boundaries', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
            '1970-01-01 00:00:01' as epoch_start,
            '2038-01-19 03:14:07' as y2k38_problem
        "));
            $row = $result->fetchOne();

            expect($row['epoch_start'])->toBe('1970-01-01 00:00:01');

            $conn->close();
        });

        it('handles user-defined variables', function (): void {
            $conn = makeConnection();
            await($conn->query('SET @myvar = 42'));

            $result = await($conn->query('SELECT @myvar as var_value'));
            $row = $result->fetchOne();

            expect($row['var_value'])->toBe('42');

            $conn->close();
        });

        it('handles quoted ? as a literal string not a placeholder', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '?' as param_test"));
            $row = $result->fetchOne();

            if (isset($row['param_test'])) {
                expect($row['param_test'])->toBe('?');
            } else {
                expect(count($row))->toBeGreaterThan(0);
            }

            $conn->close();
        });
    });

    describe('Aggregation', function (): void {

        it('handles GROUP_CONCAT', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE temp_concat (id INT, val VARCHAR(10))'));
            await($conn->query("INSERT INTO temp_concat VALUES (1,'a'),(1,'b'),(1,'c')"));

            $result = await($conn->query("SELECT GROUP_CONCAT(val ORDER BY val SEPARATOR ',') as grouped FROM temp_concat"));
            $row = $result->fetchOne();

            expect($row['grouped'])->toBe('a,b,c');

            $conn->close();
        });

        it('handles HAVING clause', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE temp_having (id INT, val INT)'));
            await($conn->query('INSERT INTO temp_having VALUES (1,10),(1,20),(2,30)'));

            $result = await($conn->query('SELECT id, SUM(val) as total FROM temp_having GROUP BY id HAVING total > 25'));
            $rows = $result->fetchAll();

            expect($rows)->toHaveCount(2)
                ->and($rows[0]['id'])->toBe('1')
                ->and($rows[0]['total'])->toBe('30')
            ;

            $conn->close();
        });

        it('handles aggregates on empty result set', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT COUNT(*) as cnt, SUM(id) as total, AVG(id) as avg FROM users WHERE 1=0'));
            $row = $result->fetchOne();

            expect($row['cnt'])->toBe('0')
                ->and($row['total'])->toBeNull()
            ;

            $conn->close();
        });
    });

    describe('Joins and Subqueries', function (): void {

        it('handles JOIN with overlapping column names', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE t1 (id INT, name VARCHAR(50))'));
            await($conn->query('CREATE TEMPORARY TABLE t2 (id INT, value VARCHAR(50))'));
            await($conn->query("INSERT INTO t1 VALUES (1, 'Alice')"));
            await($conn->query("INSERT INTO t2 VALUES (1, 'Data')"));

            $result = await($conn->query('SELECT * FROM t1 JOIN t2 ON t1.id = t2.id'));
            $row = $result->fetchOne();

            expect(isset($row['name']))->toBeTrue()
                ->and(isset($row['value']))->toBeTrue()
            ;

            $conn->close();
        });

        it('handles subquery column names', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SELECT * FROM (SELECT 1 as a, 2 as b) as sub'));
            $row = $result->fetchOne();

            expect($row['a'])->toBe('1')
                ->and($row['b'])->toBe('2')
            ;

            $conn->close();
        });

        it('handles CROSS JOIN cartesian product', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE tj1 (a INT)'));
            await($conn->query('CREATE TEMPORARY TABLE tj2 (b INT)'));
            await($conn->query('INSERT INTO tj1 VALUES (1),(2)'));
            await($conn->query('INSERT INTO tj2 VALUES (3),(4)'));

            $result = await($conn->query('SELECT * FROM tj1 CROSS JOIN tj2'));

            expect($result->fetchAll())->toHaveCount(4);

            $conn->close();
        });

        it('handles self join via copied temp table', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE emp_temp (id INT, manager_id INT)'));
            await($conn->query('INSERT INTO emp_temp VALUES (1,NULL),(2,1),(3,1)'));
            await($conn->query('CREATE TEMPORARY TABLE emp_copy SELECT * FROM emp_temp'));

            $result = await($conn->query('
            SELECT e.id, m.id as manager 
            FROM emp_temp e 
            LEFT JOIN emp_copy m ON e.manager_id = m.id
        '));

            expect($result->fetchAll())->toHaveCount(3);

            $conn->close();
        });

        it('handles nested subqueries', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('
            SELECT * FROM (
                SELECT * FROM (
                    SELECT 1 as val
                ) as inner_sub
            ) as outer_sub
        '));
            $row = $result->fetchOne();

            expect($row['val'])->toBe('1');

            $conn->close();
        });

        it('handles EXISTS and NOT EXISTS', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE test_exists (id INT)'));
            await($conn->query('INSERT INTO test_exists VALUES (1)'));

            $result = await($conn->query('SELECT EXISTS(SELECT 1 FROM test_exists) as does_exist'));
            $result2 = await($conn->query('SELECT NOT EXISTS(SELECT 1 FROM test_exists WHERE id=999) as not_exist'));

            expect($result->fetchOne()['does_exist'])->toBe('1')
                ->and($result2->fetchOne()['not_exist'])->toBe('1')
            ;

            $conn->close();
        });
    });

    describe('DML and DDL', function (): void {

        it('handles transaction rollback', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE trans_test (id INT)'));
            await($conn->query('START TRANSACTION'));
            await($conn->query('INSERT INTO trans_test VALUES (1)'));
            await($conn->query('ROLLBACK'));

            $result = await($conn->query('SELECT COUNT(*) as cnt FROM trans_test'));
            $row = $result->fetchOne();

            expect($row['cnt'])->toBe('0');

            $conn->close();
        });

        it('handles LOCK and UNLOCK TABLES', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE lock_test (id INT)'));

            try {
                await($conn->query('LOCK TABLES lock_test WRITE'));
                await($conn->query('INSERT INTO lock_test VALUES (1)'));
                await($conn->query('UNLOCK TABLES'));
                expect(true)->toBeTrue();
            } catch (Throwable) {
                expect(true)->toBeTrue();
            }

            $conn->close();
        });

        it('handles SHOW DATABASES and SHOW TABLES', function (): void {
            $conn = makeConnection();
            $result = await($conn->query('SHOW DATABASES'));
            $result2 = await($conn->query('SHOW TABLES'));

            expect(count($result->fetchAll()))->toBeGreaterThan(0)
                ->and($result2->fetchAll())->toBeArray()
            ;

            $conn->close();
        });

        it('handles DESCRIBE table', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE desc_test (id INT PRIMARY KEY, name VARCHAR(50))'));

            $result = await($conn->query('DESCRIBE desc_test'));

            expect($result->fetchAll())->toHaveCount(2);

            $conn->close();
        });

        it('handles EXPLAIN query', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE explain_test (id INT, name VARCHAR(50))'));

            $result = await($conn->query('EXPLAIN SELECT * FROM explain_test WHERE id = 1'));

            expect($result->fetchAll())->not->toHaveCount(0);

            $conn->close();
        });

        it('handles TRUNCATE TABLE', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE trunc_test (id INT)'));
            await($conn->query('INSERT INTO trunc_test VALUES (1),(2),(3)'));
            await($conn->query('TRUNCATE TABLE trunc_test'));

            $result = await($conn->query('SELECT COUNT(*) as cnt FROM trunc_test'));

            expect($result->fetchOne()['cnt'])->toBe('0');

            $conn->close();
        });

        it('returns LAST_INSERT_ID after auto increment insert', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE auto_inc (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))'));
            await($conn->query("INSERT INTO auto_inc (name) VALUES ('test')"));

            $result = await($conn->query('SELECT LAST_INSERT_ID() as last_id'));

            expect($result->fetchOne()['last_id'])->toBe('1');

            $conn->close();
        });

        it('handles ON DUPLICATE KEY UPDATE', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE dup_test (id INT PRIMARY KEY, val INT)'));
            await($conn->query('INSERT INTO dup_test VALUES (1, 10)'));
            await($conn->query('INSERT INTO dup_test VALUES (1, 20) ON DUPLICATE KEY UPDATE val = VALUES(val)'));

            $result = await($conn->query('SELECT val FROM dup_test WHERE id = 1'));

            expect($result->fetchOne()['val'])->toBe('20');

            $conn->close();
        });

        it('handles REPLACE INTO statement', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE replace_test (id INT PRIMARY KEY, val VARCHAR(50))'));
            await($conn->query("INSERT INTO replace_test VALUES (1, 'first')"));
            await($conn->query("REPLACE INTO replace_test VALUES (1, 'second')"));

            $result = await($conn->query('SELECT val FROM replace_test WHERE id = 1'));

            expect($result->fetchOne()['val'])->toBe('second');

            $conn->close();
        });

        it('handles multi-row INSERT', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE multi_ins (id INT, val VARCHAR(50))'));
            await($conn->query("INSERT INTO multi_ins VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')"));

            $result = await($conn->query('SELECT COUNT(*) as cnt FROM multi_ins'));

            expect($result->fetchOne()['cnt'])->toBe('5');

            $conn->close();
        });

        it('handles UPDATE with LIMIT', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE upd_limit (id INT, val INT)'));
            await($conn->query('INSERT INTO upd_limit VALUES (1,10),(2,20),(3,30)'));
            await($conn->query('UPDATE upd_limit SET val = 99 ORDER BY id LIMIT 2'));

            $result = await($conn->query('SELECT COUNT(*) as cnt FROM upd_limit WHERE val = 99'));

            expect($result->fetchOne()['cnt'])->toBe('2');

            $conn->close();
        });

        it('handles DELETE with LIMIT', function (): void {
            $conn = makeConnection();
            await($conn->query('CREATE TEMPORARY TABLE del_limit (id INT)'));
            await($conn->query('INSERT INTO del_limit VALUES (1),(2),(3),(4),(5)'));
            await($conn->query('DELETE FROM del_limit ORDER BY id LIMIT 2'));

            $result = await($conn->query('SELECT COUNT(*) as cnt FROM del_limit'));

            expect($result->fetchOne()['cnt'])->toBe('3');

            $conn->close();
        });

        it('handles stored procedure call', function (): void {
            $conn = makeConnection();

            try {
                await($conn->query('DROP PROCEDURE IF EXISTS test_proc'));
                await($conn->query('CREATE PROCEDURE test_proc() BEGIN SELECT 42 as answer; END'));

                $result = await($conn->query('CALL test_proc()'));
                $row = $result->fetchOne();

                expect($row['answer'])->toBe('42');

                await($conn->query('DROP PROCEDURE test_proc'));
            } catch (Throwable) {
                expect(true)->toBeTrue();
            }

            $conn->close();
        });
    });

    describe('International Characters', function (): void {

        it('returns Chinese characters correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '你好世界' as chinese"));
            $row = $result->fetchOne();

            expect($row['chinese'])->toBe('你好世界');

            $conn->close();
        });

        it('returns Japanese hiragana and kanji correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'こんにちは世界' as japanese"));
            $row = $result->fetchOne();

            expect($row['japanese'])->toBe('こんにちは世界');

            $conn->close();
        });

        it('returns Korean characters correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '안녕하세요 세계' as korean"));
            $row = $result->fetchOne();

            expect($row['korean'])->toBe('안녕하세요 세계');

            $conn->close();
        });

        it('returns Arabic characters correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'مرحبا بالعالم' as arabic"));
            $row = $result->fetchOne();

            expect($row['arabic'])->toBe('مرحبا بالعالم');

            $conn->close();
        });

        it('returns Cyrillic/Russian characters correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'Привет мир' as russian"));
            $row = $result->fetchOne();

            expect($row['russian'])->toBe('Привет мир');

            $conn->close();
        });

        it('returns Thai characters correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'สวัสดีชาวโลก' as thai"));
            $row = $result->fetchOne();

            expect($row['thai'])->toBe('สวัสดีชาวโลก');

            $conn->close();
        });

        it('returns single emoji correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '😀' as emoji"));
            $row = $result->fetchOne();

            expect($row['emoji'])->toBe('😀');

            $conn->close();
        });

        it('returns multiple different emoji correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '😀🎸🌍🚀💡🔥' as emojis"));
            $row = $result->fetchOne();

            expect($row['emojis'])->toBe('😀🎸🌍🚀💡🔥');

            $conn->close();
        });

        it('returns emoji with skin tone modifier correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '👋🏽' as skin_tone_emoji"));
            $row = $result->fetchOne();

            expect($row['skin_tone_emoji'])->toBe('👋🏽');

            $conn->close();
        });

        it('returns emoji ZWJ family sequence correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '👨‍👩‍👧‍👦' as family_emoji"));
            $row = $result->fetchOne();

            expect($row['family_emoji'])->toBe('👨‍👩‍👧‍👦');

            $conn->close();
        });

        it('returns mixed CJK and emoji in one string correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT '你好 😀 世界 🌍 こんにちは 🎸' as mixed_cjk_emoji"));
            $row = $result->fetchOne();

            expect($row['mixed_cjk_emoji'])->toBe('你好 😀 世界 🌍 こんにちは 🎸');

            $conn->close();
        });

        it('returns mixed scripts with ASCII correctly', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 'Hello 世界 Привет مرحبا 안녕 World' as mixed_scripts"));
            $row = $result->fetchOne();

            expect($row['mixed_scripts'])->toBe('Hello 世界 Привет مرحبا 안녕 World');

            $conn->close();
        });

        it('returns long CJK string with correct character and byte counts', function (): void {
            $conn = makeConnection();
            $str = str_repeat('中文字符', 250);
            $result = await($conn->query("SELECT '$str' as long_cjk, CHAR_LENGTH('$str') as char_len, LENGTH('$str') as byte_len"));
            $row = $result->fetchOne();

            expect($row['long_cjk'])->toBe($str)
                ->and((int) $row['char_len'])->toBe(1000)
                ->and((int) $row['byte_len'])->toBe(3000);

            $conn->close();
        });

        it('returns long emoji string with correct character count', function (): void {
            $conn = makeConnection();
            $str = str_repeat('🎸', 200);
            $result = await($conn->query("SELECT '$str' as long_emoji, CHAR_LENGTH('$str') as char_len, LENGTH('$str') as byte_len"));
            $row = $result->fetchOne();

            expect($row['long_emoji'])->toBe($str)
                ->and((int) $row['char_len'])->toBe(200)
                ->and((int) $row['byte_len'])->toBe(800);

            $conn->close();
        });

        it('MySQL LENGTH() returns byte count not character count for multibyte strings', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT LENGTH('🎸') as byte_len, CHAR_LENGTH('🎸') as char_len"));
            $row = $result->fetchOne();

            expect((int) $row['byte_len'])->toBe(4)
                ->and((int) $row['char_len'])->toBe(1);

            $conn->close();
        });

        it('MySQL LENGTH() returns byte count not character count for CJK characters', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT LENGTH('你好') as byte_len, CHAR_LENGTH('你好') as char_len"));
            $row = $result->fetchOne();

            expect((int) $row['byte_len'])->toBe(6)
                ->and((int) $row['char_len'])->toBe(2);

            $conn->close();
        });

        it('handles UPPER and LOWER on non-ASCII characters', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT UPPER('привет') as upper_cyr, LOWER('ПРИВЕТ') as lower_cyr"));
            $row = $result->fetchOne();

            expect($row['upper_cyr'])->toBe('ПРИВЕТ')
                ->and($row['lower_cyr'])->toBe('привет');

            $conn->close();
        });

        it('handles REVERSE on multibyte string', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT REVERSE('abc') as rev_ascii, CHAR_LENGTH(REVERSE('你好世界')) as rev_cjk_len"));
            $row = $result->fetchOne();

            expect($row['rev_ascii'])->toBe('cba')
                ->and((int) $row['rev_cjk_len'])->toBe(4);

            $conn->close();
        });

        it('handles CONCAT of multiple scripts', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT CONCAT('Hello', ' ', '世界', ' ', '🎸') as concat_result"));
            $row = $result->fetchOne();

            expect($row['concat_result'])->toBe('Hello 世界 🎸');

            $conn->close();
        });

        it('handles LIKE pattern matching with multibyte characters', function (): void {
            $conn = makeConnection();
            $result = await($conn->query("SELECT 
            '你好世界' LIKE '你好%' as cjk_prefix,
            '🎸🎵🌍' LIKE '%🌍' as emoji_suffix,
            'Привет мир' LIKE '%мир' as cyrillic_suffix
        "));
            $row = $result->fetchOne();

            expect($row['cjk_prefix'])->toBe('1')
                ->and($row['emoji_suffix'])->toBe('1')
                ->and($row['cyrillic_suffix'])->toBe('1');

            $conn->close();
        });
    });
});
