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

describe('PreparedStatement', function (): void {

    describe('Basic', function (): void {

        it('executes a basic SELECT with a parameter', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as num'));
            $result = await($stmt->execute([42]));
            $row = $result->fetchOne();

            expect($row['num'])->toBe(42);

            await($stmt->close());
            $conn->close();
        });

        it('handles all null parameters', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?'));
            $result = await($stmt->execute([null, null, null]));
            $row = $result->fetchOne();

            expect($row)->toHaveKey('?')
                ->and($row)->toHaveKey('?1')
                ->and($row)->toHaveKey('?2')
                ->and($row['?'])->toBeNull()
                ->and($row['?1'])->toBeNull()
                ->and($row['?2'])->toBeNull()
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles 7 columns with alternating nulls', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute([1, null, 3, null, 5, null, 7]));
            $row = $result->fetchOne();

            expect($row['?'])->toBe(1)
                ->and($row['?1'])->toBeNull()
                ->and($row['?2'])->toBe(3)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles 8 columns', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute([1, 2, 3, 4, 5, 6, 7, 8]));
            $row = $result->fetchOne();

            expect($row['?'])->toBe(1)
                ->and($row['?7'])->toBe(8)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles many columns', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute(range(1, 23)));
            $row = $result->fetchOne();

            expect($row['?'])->toBe(1)
                ->and($row['?22'])->toBe(23)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles alternating nulls across 8 columns', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute([1, null, 3, null, 5, null, 7, null]));
            $row = $result->fetchOne();

            expect($row['?'])->toBe(1)
                ->and($row['?1'])->toBeNull()
                ->and($row['?2'])->toBe(3)
                ->and($row['?3'])->toBeNull()
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles empty string parameter', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as empty_str'));
            $result = await($stmt->execute(['']));
            $row = $result->fetchOne();

            expect($row['empty_str'])->toBe('');

            await($stmt->close());
            $conn->close();
        });

        it('handles zero values', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as zero_int, ? as zero_float'));
            $result = await($stmt->execute([0, 0.0]));
            $row = $result->fetchOne();

            expect($row['zero_int'])->toBe(0)
                ->and($row['zero_float'])->toBe(0.0)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles mixed types with nulls', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as int_val, ? as str_val, ? as null_val, ? as float_val'));
            $result = await($stmt->execute([42, 'hello', null, 3.14]));
            $row = $result->fetchOne();

            expect($row['int_val'])->toBe(42)
                ->and($row['str_val'])->toBe('hello')
                ->and($row['null_val'])->toBeNull()
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles first and last as null', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?'));
            $result = await($stmt->execute([null, 2, 3, 4, null]));
            $row = $result->fetchOne();

            expect($row)->toHaveKey('?')
                ->and($row)->toHaveKey('?1')
                ->and($row)->toHaveKey('?4')
                ->and($row['?'])->toBeNull()
                ->and($row['?1'])->toBe(2)
                ->and($row['?4'])->toBeNull()
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles single column null', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as single_null'));
            $result = await($stmt->execute([null]));
            $row = $result->fetchOne();

            expect($row['single_null'])->toBeNull();

            await($stmt->close());
            $conn->close();
        });

        it('returns multiple rows', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT 1 UNION SELECT 2 UNION SELECT 3'));
            $result = await($stmt->execute([]));

            expect($result->fetchAll())->toHaveCount(3);

            await($stmt->close());
            $conn->close();
        });

        it('returns empty result set', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT * FROM users WHERE id = ?'));
            $result = await($stmt->execute([999999]));

            expect($result->fetchAll())->toHaveCount(0);

            await($stmt->close());
            $conn->close();
        });
    });

    describe('Integer Boundaries', function (): void {

        it('handles TINYINT boundaries', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS SIGNED) as min_val, CAST(? AS SIGNED) as max_val'));
            $result = await($stmt->execute([-128, 127]));
            $row = $result->fetchOne();

            expect($row['min_val'])->toBe(-128)
                ->and($row['max_val'])->toBe(127)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles SMALLINT boundaries', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS SIGNED) as min_val, CAST(? AS SIGNED) as max_val'));
            $result = await($stmt->execute([-32768, 32767]));
            $row = $result->fetchOne();

            expect($row['min_val'])->toBe(-32768)
                ->and($row['max_val'])->toBe(32767)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles MEDIUMINT boundaries', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS SIGNED) as min_val, CAST(? AS SIGNED) as max_val'));
            $result = await($stmt->execute([-8388608, 8388607]));
            $row = $result->fetchOne();

            expect($row['min_val'])->toBe(-8388608)
                ->and($row['max_val'])->toBe(8388607)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles INT boundaries', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS SIGNED) as min_val, CAST(? AS SIGNED) as max_val'));
            $result = await($stmt->execute([-2147483648, 2147483647]));
            $row = $result->fetchOne();

            expect($row['min_val'])->toBe(-2147483648)
                ->and($row['max_val'])->toBe(2147483647)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles BIGINT boundaries', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS SIGNED) as min_val, CAST(? AS SIGNED) as max_val'));
            $result = await($stmt->execute([PHP_INT_MIN, PHP_INT_MAX]));
            $row = $result->fetchOne();

            expect($row['min_val'])->toBe(PHP_INT_MIN)
                ->and($row['max_val'])->toBe(PHP_INT_MAX)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles large integers (PHP_INT_MAX)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as big_int'));
            $result = await($stmt->execute([PHP_INT_MAX]));
            $row = $result->fetchOne();

            expect($row['big_int'])->toBe(PHP_INT_MAX);

            await($stmt->close());
            $conn->close();
        });

        it('handles negative integers', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as negative'));
            $result = await($stmt->execute([-42]));
            $row = $result->fetchOne();

            expect($row['negative'])->toBe(-42);

            await($stmt->close());
            $conn->close();
        });

        it('handles unsigned TINYINT max (255)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS UNSIGNED) as val'));
            $result = await($stmt->execute([255]));
            $row = $result->fetchOne();

            expect($row['val'])->toBe(255);

            await($stmt->close());
            $conn->close();
        });

        it('handles unsigned SMALLINT max (65535)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS UNSIGNED) as val'));
            $result = await($stmt->execute([65535]));
            $row = $result->fetchOne();

            expect($row['val'])->toBe(65535);

            await($stmt->close());
            $conn->close();
        });

        it('handles unsigned INT max (4294967295)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS UNSIGNED) as val'));
            $result = await($stmt->execute([4294967295]));
            $row = $result->fetchOne();

            expect($row['val'])->toBe(4294967295);

            await($stmt->close());
            $conn->close();
        });

        it('handles unsigned BIGINT max as string or int', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(18446744073709551615 AS UNSIGNED) as val'));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect(is_string($row['val']) || is_int($row['val']))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });
    });

    describe('Float / Double', function (): void {

        it('handles float precision', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as float_val'));
            $result = await($stmt->execute([3.14159]));
            $row = $result->fetchOne();

            expect(abs($row['float_val'] - 3.14159))->toBeLessThan(0.001);

            await($stmt->close());
            $conn->close();
        });

        it('handles NaN (converts to null or 0)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as nan_val'));
            $result = await($stmt->execute([NAN]));
            $row = $result->fetchOne();

            expect($row['nan_val'] === null || $row['nan_val'] === 0.0 || is_nan((float) $row['nan_val']))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles INF (converts to null or infinite)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as inf_val'));
            $result = await($stmt->execute([INF]));
            $row = $result->fetchOne();

            expect($row['inf_val'] === null || is_infinite((float) $row['inf_val']))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles negative INF', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as neg_inf_val'));
            $result = await($stmt->execute([-INF]));
            $row = $result->fetchOne();

            expect($row['neg_inf_val'] === null || is_infinite((float) $row['neg_inf_val']))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles very small double (1.23e-300)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as small_val'));
            $result = await($stmt->execute([1.23e-300]));
            $row = $result->fetchOne();

            expect(is_float($row['small_val']))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles very large double (1.23e308)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as large_val'));
            $result = await($stmt->execute([1.23e308]));
            $row = $result->fetchOne();

            expect(is_float($row['large_val']) || is_infinite($row['large_val']))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles negative zero (-0.0)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as neg_zero'));
            $result = await($stmt->execute([-0.0]));
            $row = $result->fetchOne();

            expect($row['neg_zero'])->toBe(0.0);

            await($stmt->close());
            $conn->close();
        });
    });

    describe('String Edge Cases', function (): void {

        it('handles binary data', function (): void {
            $conn = makeConnection();
            $binary = "\x00\x01\x02\xFF";
            $stmt = await($conn->prepare('SELECT ? as binary_data'));
            $result = await($stmt->execute([$binary]));
            $row = $result->fetchOne();

            expect($row['binary_data'])->toBe($binary);

            await($stmt->close());
            $conn->close();
        });

        it('handles long binary (10KB)', function (): void {
            $conn = makeConnection();
            $longBinary = str_repeat("\x00\xFF", 5000);
            $stmt = await($conn->prepare('SELECT ? as long_binary'));
            $result = await($stmt->execute([$longBinary]));
            $row = $result->fetchOne();

            expect(strlen($row['long_binary']))->toBe(10000);

            await($stmt->close());
            $conn->close();
        });

        it('handles emoji', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as emoji'));
            $result = await($stmt->execute(['🎸🎵']));
            $row = $result->fetchOne();

            expect($row['emoji'])->toBe('🎸🎵');

            await($stmt->close());
            $conn->close();
        });

        it('handles mixed unicode', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as unicode'));
            $result = await($stmt->execute(['Hello 世界 🌍']));
            $row = $result->fetchOne();

            expect($row['unicode'])->toBe('Hello 世界 🌍');

            await($stmt->close());
            $conn->close();
        });

        it('handles very long string (1MB)', function (): void {
            $conn = makeConnection();
            $longStr = str_repeat('A', 1000000);
            $stmt = await($conn->prepare('SELECT ? as long_str'));
            $result = await($stmt->execute([$longStr]));
            $row = $result->fetchOne();

            expect(strlen($row['long_str']))->toBe(1000000);

            await($stmt->close());
            $conn->close();
        });

        it('handles null byte in string', function (): void {
            $conn = makeConnection();
            $str = "before\x00after";
            $stmt = await($conn->prepare('SELECT ? as null_byte_str'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['null_byte_str'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles string with single and double quotes', function (): void {
            $conn = makeConnection();
            $str = "He said \"Hello\" and she said 'Hi'";
            $stmt = await($conn->prepare('SELECT ? as quoted'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['quoted'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles string with backslashes', function (): void {
            $conn = makeConnection();
            $str = 'C:\\path\\to\\file\\test.txt';
            $stmt = await($conn->prepare('SELECT ? as backslashes'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['backslashes'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles string with newlines', function (): void {
            $conn = makeConnection();
            $str = "Line1\nLine2\rLine3\r\nLine4";
            $stmt = await($conn->prepare('SELECT ? as newlines'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['newlines'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles 4-byte UTF-8 characters', function (): void {
            $conn = makeConnection();
            $str = '𝕳𝖊𝖑𝖑𝖔';
            $stmt = await($conn->prepare('SELECT ? as four_byte'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['four_byte'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles mixed line endings', function (): void {
            $conn = makeConnection();
            $str = "Unix\nWindows\r\nMac\rMixed";
            $stmt = await($conn->prepare('SELECT ? as mixed'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['mixed'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });
    });

    // ---------------------------------------------------------------------------
    // Date / Time
    // ---------------------------------------------------------------------------

    describe('Date / Time', function (): void {

        it('handles zero date', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('0000-00-00' AS DATE) as zero_date"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['zero_date'] === '0000-00-00' || $row['zero_date'] === null)->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles datetime with microseconds', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('2025-02-01 12:34:56.123456' AS DATETIME(6)) as dt"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect(str_starts_with($row['dt'], '2025-02-01 12:34:56'))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles negative time (-838:59:59)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('-838:59:59' AS TIME) as neg_time"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['neg_time'])->toBe('-838:59:59');

            await($stmt->close());
            $conn->close();
        });

        it('handles long time (100:30:45)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('100:30:45' AS TIME) as long_time"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['long_time'])->toBe('100:30:45');

            await($stmt->close());
            $conn->close();
        });

        it('handles max date (9999-12-31)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('9999-12-31' AS DATE) as max_date"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['max_date'])->toBe('9999-12-31');

            await($stmt->close());
            $conn->close();
        });

        it('handles datetime epoch (1970-01-01 00:00:00)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('1970-01-01 00:00:00' AS DATETIME) as epoch"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['epoch'])->toBe('1970-01-01 00:00:00');

            await($stmt->close());
            $conn->close();
        });

        it('handles far future datetime (2099-12-31 23:59:59)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('2099-12-31 23:59:59' AS DATETIME) as future"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['future'])->toBe('2099-12-31 23:59:59');

            await($stmt->close());
            $conn->close();
        });

        it('handles time with microseconds', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('12:34:56.789012' AS TIME(6)) as time_micro"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect(str_starts_with($row['time_micro'], '12:34:56'))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });

        it('handles timestamp near 2038 boundary', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('2038-01-19 03:14:07' AS DATETIME) as near_limit"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect(str_starts_with($row['near_limit'], '2038-01-19'))->toBeTrue();

            await($stmt->close());
            $conn->close();
        });
    });

    // ---------------------------------------------------------------------------
    // NULL Bitmap
    // ---------------------------------------------------------------------------

    describe('NULL Bitmap', function (): void {

        it('handles 16 columns all null (2-byte bitmap boundary)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute(array_fill(0, 16, null)));
            $row = $result->fetchOne();

            expect(count($row))->toBe(16);

            await($stmt->close());
            $conn->close();
        });

        it('handles 17 columns all null (3-byte bitmap boundary)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute(array_fill(0, 17, null)));
            $row = $result->fetchOne();

            expect(count($row))->toBe(17);

            await($stmt->close());
            $conn->close();
        });

        it('handles null bitmap with all bits set (10 nulls)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute(array_fill(0, 10, null)));
            $row = $result->fetchOne();

            foreach ($row as $val) {
                expect($val)->toBeNull();
            }

            await($stmt->close());
            $conn->close();
        });

        it('handles null bitmap with no bits set (10 values)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'));
            $result = await($stmt->execute(range(1, 10)));
            $row = $result->fetchOne();

            foreach ($row as $val) {
                expect($val)->not->toBeNull();
            }

            await($stmt->close());
            $conn->close();
        });
    });

    describe('Statement Reuse', function (): void {

        it('reuses a statement with the same param type', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as val'));

            $row1 = await($stmt->execute([100]));
            $row2 = await($stmt->execute([200]));
            $row3 = await($stmt->execute([300]));

            expect($row1->fetchOne()['val'])->toBe(100)
                ->and($row2->fetchOne()['val'])->toBe(200)
                ->and($row3->fetchOne()['val'])->toBe(300)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('reuses a statement with different param types', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as int_val, ? as str_val'));

            $result1 = await($stmt->execute([42, 'hello']));
            $result2 = await($stmt->execute([99, 'world']));

            expect($result1->fetchOne()['int_val'])->toBe(42)
                ->and($result1->fetchOne()['str_val'])->toBe('hello')
                ->and($result2->fetchOne()['int_val'])->toBe(99)
                ->and($result2->fetchOne()['str_val'])->toBe('world')
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles concurrent statements on the same connection', function (): void {
            $conn = makeConnection();
            $stmt1 = await($conn->prepare('SELECT ? as val1'));
            $stmt2 = await($conn->prepare('SELECT ? as val2'));

            $result1 = await($stmt1->execute([111]));
            $result2 = await($stmt2->execute([222]));

            expect($result1->fetchOne()['val1'])->toBe(111)
                ->and($result2->fetchOne()['val2'])->toBe(222)
            ;

            await($stmt1->close());
            await($stmt2->close());
            $conn->close();
        });
    });

    describe('Complex Queries', function (): void {

        it('handles JOIN with params', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT u.id FROM users u WHERE u.id = ?'));

            $result = await($stmt->execute([1]));

            expect($result->fetchAll())->toBeArray();

            await($stmt->close());
            $conn->close();
        });

        it('handles subquery with params', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT * FROM (SELECT ? as val) sub WHERE sub.val > 0'));
            $result = await($stmt->execute([42]));
            $row = $result->fetchOne();

            expect($row['val'])->toBe(42);

            await($stmt->close());
            $conn->close();
        });

        it('handles ORDER BY with params in UNION', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? as val UNION SELECT ? UNION SELECT ? ORDER BY val'));
            $result = await($stmt->execute([3, 1, 2]));
            $rows = $result->fetchAll();

            expect($rows[0]['val'])->toBe(1)
                ->and($rows[1]['val'])->toBe(2)
                ->and($rows[2]['val'])->toBe(3)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles LIKE pattern matching', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT ? LIKE ? as matches'));
            $result = await($stmt->execute(['hello world', '%world%']));
            $row = $result->fetchOne();

            expect($row['matches'])->toBe(1);

            await($stmt->close());
            $conn->close();
        });
    });

    describe('DECIMAL', function (): void {

        it('handles decimal precision', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('123.456789' AS DECIMAL(10,6)) as dec_val"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['dec_val'])->toBe('123.456789');

            await($stmt->close());
            $conn->close();
        });

        it('handles negative decimal', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('-999.999' AS DECIMAL(6,3)) as dec_val"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['dec_val'])->toBe('-999.999');

            await($stmt->close());
            $conn->close();
        });

        it('handles large decimal', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare("SELECT CAST('99999999999999.99' AS DECIMAL(16,2)) as dec_val"));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['dec_val'])->toBe('99999999999999.99');

            await($stmt->close());
            $conn->close();
        });
    });

    describe('BLOB', function (): void {

        it('handles TINY BLOB (255 bytes)', function (): void {
            $conn = makeConnection();
            $data = str_repeat('X', 255);
            $stmt = await($conn->prepare('SELECT CAST(? AS BINARY(255)) as blob_val'));
            $result = await($stmt->execute([$data]));
            $row = $result->fetchOne();

            expect(strlen($row['blob_val']))->toBe(255);

            await($stmt->close());
            $conn->close();
        });

        it('handles MEDIUM BLOB (10KB)', function (): void {
            $conn = makeConnection();
            $data = str_repeat('M', 10000);
            $stmt = await($conn->prepare('SELECT ? as blob_val'));
            $result = await($stmt->execute([$data]));
            $row = $result->fetchOne();

            expect(strlen($row['blob_val']))->toBe(10000);

            await($stmt->close());
            $conn->close();
        });

        it('handles LONG BLOB (100KB)', function (): void {
            $conn = makeConnection();
            $data = str_repeat('L', 100000);
            $stmt = await($conn->prepare('SELECT ? as blob_val'));
            $result = await($stmt->execute([$data]));
            $row = $result->fetchOne();

            expect(strlen($row['blob_val']))->toBe(100000);

            await($stmt->close());
            $conn->close();
        });
    });

    describe('JSON', function (): void {

        it('handles JSON object', function (): void {
            $conn = makeConnection();
            $json = '{"name":"John","age":30}';
            $stmt = await($conn->prepare('SELECT CAST(? AS JSON) as json_val'));
            $result = await($stmt->execute([$json]));
            $row = $result->fetchOne();
            $decoded = json_decode($row['json_val'], true);

            expect($decoded['name'])->toBe('John')
                ->and($decoded['age'])->toBe(30)
            ;

            await($stmt->close());
            $conn->close();
        });

        it('handles JSON array', function (): void {
            $conn = makeConnection();
            $json = '[1,2,3,4,5]';
            $stmt = await($conn->prepare('SELECT CAST(? AS JSON) as json_val'));
            $result = await($stmt->execute([$json]));
            $row = $result->fetchOne();
            $decoded = json_decode($row['json_val'], true);

            expect($decoded)->toBe([1, 2, 3, 4, 5]);

            await($stmt->close());
            $conn->close();
        });

        it('handles nested JSON', function (): void {
            $conn = makeConnection();
            $json = '{"user":{"name":"Alice","roles":["admin","user"]}}';
            $stmt = await($conn->prepare('SELECT CAST(? AS JSON) as json_val'));
            $result = await($stmt->execute([$json]));
            $row = $result->fetchOne();
            $decoded = json_decode($row['json_val'], true);

            expect($decoded['user']['name'])->toBe('Alice')
                ->and($decoded['user']['roles'][0])->toBe('admin')
            ;

            await($stmt->close());
            $conn->close();
        });
    });

    describe('BIT', function (): void {

        it('handles BIT field value (0b11010110 = 214)', function (): void {
            $conn = makeConnection();
            $stmt = await($conn->prepare('SELECT CAST(? AS UNSIGNED) as bit_val'));
            $result = await($stmt->execute([0b11010110]));
            $row = $result->fetchOne();

            expect($row['bit_val'])->toBe(214);

            await($stmt->close());
            $conn->close();
        });
    });

    describe('ENUM and SET', function (): void {

        it('handles ENUM type', function (): void {
            $conn = makeConnection();

            await($conn->query("CREATE TEMPORARY TABLE test_enum (val ENUM('small','medium','large'))"));
            await($conn->query("INSERT INTO test_enum VALUES ('medium')"));

            $stmt = await($conn->prepare('SELECT val FROM test_enum'));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect($row['val'])->toBe('medium');

            await($stmt->close());
            $conn->close();
        });

        it('handles SET type', function (): void {
            $conn = makeConnection();

            await($conn->query("CREATE TEMPORARY TABLE test_set (tags SET('php','mysql','javascript'))"));
            await($conn->query("INSERT INTO test_set VALUES ('php,mysql')"));

            $stmt = await($conn->prepare('SELECT tags FROM test_set'));
            $result = await($stmt->execute([]));
            $row = $result->fetchOne();

            expect(str_contains($row['tags'], 'php'))->toBeTrue()
                ->and(str_contains($row['tags'], 'mysql'))->toBeTrue()
            ;

            await($stmt->close());
            $conn->close();
        });
    });

    describe('International Characters', function (): void {

        it('handles Chinese characters', function (): void {
            $conn = makeConnection();
            $str = '你好世界';
            $stmt = await($conn->prepare('SELECT ? as chinese'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['chinese'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles Japanese hiragana and kanji', function (): void {
            $conn = makeConnection();
            $str = 'こんにちは世界';
            $stmt = await($conn->prepare('SELECT ? as japanese'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['japanese'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles Korean characters', function (): void {
            $conn = makeConnection();
            $str = '안녕하세요 세계';
            $stmt = await($conn->prepare('SELECT ? as korean'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['korean'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles Arabic characters', function (): void {
            $conn = makeConnection();
            $str = 'مرحبا بالعالم';
            $stmt = await($conn->prepare('SELECT ? as arabic'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['arabic'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles Russian/Cyrillic characters', function (): void {
            $conn = makeConnection();
            $str = 'Привет мир';
            $stmt = await($conn->prepare('SELECT ? as russian'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['russian'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles Thai characters', function (): void {
            $conn = makeConnection();
            $str = 'สวัสดีชาวโลก';
            $stmt = await($conn->prepare('SELECT ? as thai'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['thai'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles single emoji', function (): void {
            $conn = makeConnection();
            $str = '😀';
            $stmt = await($conn->prepare('SELECT ? as emoji'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['emoji'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles multiple different emoji', function (): void {
            $conn = makeConnection();
            $str = '😀🎸🌍🚀💡🔥';
            $stmt = await($conn->prepare('SELECT ? as emojis'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['emojis'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles emoji with skin tone modifier', function (): void {
            $conn = makeConnection();
            $str = '👋🏽'; 
            $stmt = await($conn->prepare('SELECT ? as skin_tone_emoji'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['skin_tone_emoji'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles emoji family sequence (ZWJ sequence)', function (): void {
            $conn = makeConnection();
            $str = '👨‍👩‍👧‍👦'; 
            $stmt = await($conn->prepare('SELECT ? as family_emoji'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['family_emoji'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles mixed CJK and emoji in one string', function (): void {
            $conn = makeConnection();
            $str = '你好 😀 世界 🌍 こんにちは 🎸';
            $stmt = await($conn->prepare('SELECT ? as mixed_cjk_emoji'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['mixed_cjk_emoji'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles mixed scripts with ASCII', function (): void {
            $conn = makeConnection();
            $str = 'Hello 世界 Привет مرحبا 안녕 World';
            $stmt = await($conn->prepare('SELECT ? as mixed_scripts'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['mixed_scripts'])->toBe($str);

            await($stmt->close());
            $conn->close();
        });

        it('handles long string of CJK characters (1000 chars)', function (): void {
            $conn = makeConnection();
            $str = str_repeat('中文字符', 250); 
            $stmt = await($conn->prepare('SELECT ? as long_cjk'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['long_cjk'])->toBe($str)
                ->and(mb_strlen($row['long_cjk']))->toBe(1000);

            await($stmt->close());
            $conn->close();
        });

        it('handles long string of emoji (200 emoji)', function (): void {
            $conn = makeConnection();
            $str = str_repeat('🎸', 200); 
            $stmt = await($conn->prepare('SELECT ? as long_emoji'));
            $result = await($stmt->execute([$str]));
            $row = $result->fetchOne();

            expect($row['long_emoji'])->toBe($str)
                ->and(mb_strlen($row['long_emoji']))->toBe(200);

            await($stmt->close());
            $conn->close();
        });

        it('preserves byte length of multibyte strings correctly', function (): void {
            $conn = makeConnection();
            $str = '🎸'; 
            $stmt = await($conn->prepare('SELECT ? as single_emoji, LENGTH(?) as byte_len'));
            $result = await($stmt->execute([$str, $str]));
            $row = $result->fetchOne();

            expect($row['single_emoji'])->toBe($str)
                ->and((int) $row['byte_len'])->toBe(4); 

            await($stmt->close());
            $conn->close();
        });

        it('preserves character count of CJK strings correctly', function (): void {
            $conn = makeConnection();
            $str = '你好世界'; 
            $stmt = await($conn->prepare('SELECT ? as cjk, CHAR_LENGTH(?) as char_len, LENGTH(?) as byte_len'));
            $result = await($stmt->execute([$str, $str, $str]));
            $row = $result->fetchOne();

            expect($row['cjk'])->toBe($str)
                ->and((int) $row['char_len'])->toBe(4) 
                ->and((int) $row['byte_len'])->toBe(12);  

            await($stmt->close());
            $conn->close();
        });
    });
});
