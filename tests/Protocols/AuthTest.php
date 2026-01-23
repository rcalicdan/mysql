<?php

use Hibla\MysqlClient\Protocols\Auth;

describe('Auth', function () {
    describe('scramblePassword - mysql_native_password', function () {
        it('returns empty string for empty password', function () {
            $nonce = str_repeat("\x00", 20);
            
            $result = Auth::scramblePassword('', $nonce);
            
            expect($result)->toBe('');
        });

        it('scrambles a simple password correctly', function () {
            $password = 'test123';
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
            expect($result)->not->toBe($password);
        });

        it('produces consistent results for same inputs', function () {
            $password = 'mypassword';
            $nonce = "\x4a\x3b\x46\x38\x68\x53\x6e\x74\x74\x55\x6c\x43\x79\x76\x6f\x54\x4c\x74\x76\x79";
            
            $result1 = Auth::scramblePassword($password, $nonce);
            $result2 = Auth::scramblePassword($password, $nonce);
            
            expect($result1)->toBe($result2);
        });

        it('produces different results for different passwords', function () {
            $nonce = random_bytes(20);
            
            $result1 = Auth::scramblePassword('password1', $nonce);
            $result2 = Auth::scramblePassword('password2', $nonce);
            
            expect($result1)->not->toBe($result2);
        });

        it('produces different results for different nonces', function () {
            $password = 'testpassword';
            
            $result1 = Auth::scramblePassword($password, random_bytes(20));
            $result2 = Auth::scramblePassword($password, random_bytes(20));
            
            expect($result1)->not->toBe($result2);
        });

        it('handles special characters in password', function () {
            $password = 'p@ssw0rd!#$%^&*()';
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('handles unicode characters in password', function () {
            $password = 'пароль密码🔐';
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('handles very long password', function () {
            $password = str_repeat('long_password_', 100);
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('handles nonce with all zeros', function () {
            $password = 'test';
            $nonce = str_repeat("\x00", 20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('handles nonce with all ones', function () {
            $password = 'test';
            $nonce = str_repeat("\xff", 20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('implements correct mysql_native_password algorithm', function () {
            $password = 'password';
            $nonce = pack('H*', '4a3b463868536e7474556c4379766f544c747679');
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
            
            $stage1 = sha1($password, true);
            $stage2 = sha1($stage1, true);
            $stage3 = sha1($nonce . $stage2, true);
            $expected = $stage1 ^ $stage3;
            
            expect($result)->toBe($expected);
        });

        it('handles password with null bytes', function () {
            $password = "pass\x00word";
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('handles single character password', function () {
            $password = 'a';
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });
    });

    describe('scrambleCachingSha2Password - caching_sha2_password', function () {
        it('returns empty string for empty password', function () {
            $nonce = str_repeat("\x00", 20);
            
            $result = Auth::scrambleCachingSha2Password('', $nonce);
            
            expect($result)->toBe('');
        });

        it('scrambles a simple password correctly', function () {
            $password = 'test123';
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
            expect($result)->not->toBe($password);
        });

        it('produces consistent results for same inputs', function () {
            $password = 'mypassword';
            $nonce = random_bytes(20);
            
            $result1 = Auth::scrambleCachingSha2Password($password, $nonce);
            $result2 = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect($result1)->toBe($result2);
        });

        it('produces different results for different passwords', function () {
            $nonce = random_bytes(20);
            
            $result1 = Auth::scrambleCachingSha2Password('password1', $nonce);
            $result2 = Auth::scrambleCachingSha2Password('password2', $nonce);
            
            expect($result1)->not->toBe($result2);
        });

        it('produces different results for different nonces', function () {
            $password = 'testpassword';
            
            $result1 = Auth::scrambleCachingSha2Password($password, random_bytes(20));
            $result2 = Auth::scrambleCachingSha2Password($password, random_bytes(20));
            
            expect($result1)->not->toBe($result2);
        });

        it('handles special characters in password', function () {
            $password = 'p@ssw0rd!#$%^&*()';
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles unicode characters in password', function () {
            $password = 'пароль密码🔐';
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles very long password', function () {
            $password = str_repeat('long_password_', 100);
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles nonce with all zeros', function () {
            $password = 'test';
            $nonce = str_repeat("\x00", 20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles nonce with all ones', function () {
            $password = 'test';
            $nonce = str_repeat("\xff", 20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('implements correct caching_sha2_password algorithm', function () {
            $password = 'password';
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            $hash1 = hash('sha256', $password, true);
            $hash2 = hash('sha256', $hash1, true);
            $hash3 = hash('sha256', $hash2 . $nonce, true);
            
            $expected = '';
            for ($i = 0; $i < strlen($hash1); $i++) {
                $expected .= chr(ord($hash1[$i]) ^ ord($hash3[$i]));
            }
            
            expect($result)->toBe($expected);
        });

        it('handles password with null bytes', function () {
            $password = "pass\x00word";
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles single character password', function () {
            $password = 'a';
            $nonce = random_bytes(20);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles nonce longer than 20 bytes', function () {
            $password = 'test';
            $nonce = random_bytes(32);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles nonce shorter than 20 bytes', function () {
            $password = 'test';
            $nonce = random_bytes(10);
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });
    });

    describe('comparison between algorithms', function () {
        it('produces different length outputs', function () {
            $password = 'samepassword';
            $nonce = random_bytes(20);
            
            $native = Auth::scramblePassword($password, $nonce);
            $caching = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($native))->toBe(20);
            
            expect(strlen($caching))->toBe(32);
        });

        it('produces different outputs for same inputs', function () {
            $password = 'password';
            $nonce = random_bytes(20);
            
            $native = Auth::scramblePassword($password, $nonce);
            $caching = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(substr($caching, 0, 20))->not->toBe($native);
        });
    });

    describe('edge cases', function () {
        it('handles empty nonce for mysql_native_password', function () {
            $password = 'test';
            $nonce = '';
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });

        it('handles empty nonce for caching_sha2_password', function () {
            $password = 'test';
            $nonce = '';
            
            $result = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($result))->toBe(32);
        });

        it('handles password with only spaces', function () {
            $password = '     ';
            $nonce = random_bytes(20);
            
            $native = Auth::scramblePassword($password, $nonce);
            $caching = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($native))->toBe(20);
            expect(strlen($caching))->toBe(32);
        });

        it('handles extremely long nonce', function () {
            $password = 'test';
            $nonce = random_bytes(1000);
            
            $native = Auth::scramblePassword($password, $nonce);
            $caching = Auth::scrambleCachingSha2Password($password, $nonce);
            
            expect(strlen($native))->toBe(20);
            expect(strlen($caching))->toBe(32);
        });

        it('ensures XOR operation is reversible concept', function () {
            $password = 'test';
            $nonce = random_bytes(20);
            
            $stage1 = sha1($password, true);
            $stage2 = sha1($stage1, true);
            $stage3 = sha1($nonce . $stage2, true);
            
            $scrambled = $stage1 ^ $stage3;
            
            $recovered = $scrambled ^ $stage3;
            
            expect($recovered)->toBe($stage1);
        });

        it('ensures different byte values in output', function () {
            $password = 'diversepassword';
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            $bytes = array_map('ord', str_split($result));
            $uniqueBytes = array_unique($bytes);
            
            expect(count($uniqueBytes))->toBeGreaterThan(1);
        });

        it('handles password matching nonce pattern', function () {
            $nonce = random_bytes(20);
            $password = bin2hex($nonce); 
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect(strlen($result))->toBe(20);
        });
    });

    describe('security properties', function () {
        it('does not leak password length in output for mysql_native_password', function () {
            $nonce = random_bytes(20);
            
            $short = Auth::scramblePassword('a', $nonce);
            $long = Auth::scramblePassword(str_repeat('a', 1000), $nonce);
            
            expect(strlen($short))->toBe(strlen($long));
            expect(strlen($short))->toBe(20);
        });

        it('does not leak password length in output for caching_sha2_password', function () {
            $nonce = random_bytes(20);
            
            $short = Auth::scrambleCachingSha2Password('a', $nonce);
            $long = Auth::scrambleCachingSha2Password(str_repeat('a', 1000), $nonce);
            
            expect(strlen($short))->toBe(strlen($long));
            expect(strlen($short))->toBe(32);
        });

        it('produces apparently random output', function () {
            $password = 'testpassword';
            $nonce = random_bytes(20);
            
            $result = Auth::scramblePassword($password, $nonce);
            
            expect($result)->not->toContain('testpassword');
        });
    });
});