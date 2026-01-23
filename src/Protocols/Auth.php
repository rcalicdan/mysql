<?php

namespace Hibla\MysqlClient\Protocols;

final class Auth
{
    /**
     * Creates the auth response for mysql_native_password.
     * It's calculated as: SHA1(password) XOR SHA1(nonce + SHA1(SHA1(password)))
     *
     * @param  string  $password  The user's password.
     * @param  string  $nonce  The scramble/nonce from the server's handshake.
     * @return string The binary scrambled password.
     */
    public static function scramblePassword(string $password, string $nonce): string
    {
        if ($password === '') {
            return '';
        }

        $stage1 = sha1($password, true);
        $stage2 = sha1($stage1, true);
        $stage3 = sha1($nonce.$stage2, true);

        return $stage1 ^ $stage3;
    }

    public static function scrambleCachingSha2Password(string $password, string $nonce): string
    {
        if ($password === '') {
            return '';
        }

        $hash1 = hash('sha256', $password, true);
        $hash2 = hash('sha256', $hash1, true);
        $hash3 = hash('sha256', $hash2.$nonce, true);

        // XOR the first hash with the third hash
        $scrambled = '';
        for ($i = 0; $i < \strlen($hash1); $i++) {
            $scrambled .= \chr(\ord($hash1[$i]) ^ \ord($hash3[$i]));
        }

        return $scrambled;
    }
}
