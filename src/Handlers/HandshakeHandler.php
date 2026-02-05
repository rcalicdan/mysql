<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Hibla\Sql\Exceptions\AuthenticationException;
use Hibla\Sql\Exceptions\ConnectionException;
use Rcalicdan\MySQLBinaryProtocol\Auth\AuthScrambler;
use Rcalicdan\MySQLBinaryProtocol\Buffer\Writer\BufferPayloadWriterFactory;
use Rcalicdan\MySQLBinaryProtocol\Constants\AuthPacketType;
use Rcalicdan\MySQLBinaryProtocol\Constants\CapabilityFlags;
use Rcalicdan\MySQLBinaryProtocol\Constants\CharsetIdentifiers;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeParser;
use Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeResponse41;
use Rcalicdan\MySQLBinaryProtocol\Packet\PayloadReader;
use Rcalicdan\MySQLBinaryProtocol\Packet\UncompressedPacketReader;

/**
 * Handles MySQL handshake protocol including SSL/TLS upgrade.
 *
 * MySQL uses STARTTLS protocol which requires upgrading an existing
 * plain-text connection to encrypted during the handshake phase.
 * This is different from most protocols which use TLS from connection start.
 *
 * Requirements:
 * - Socket must support enableEncryption(array $sslOptions): PromiseInterface
 *   for mid-connection SSL/TLS upgrade (MySQL's STARTTLS protocol)
 *
 * Note: This is a MySQL-specific requirement. Most protocols don't need
 * mid-connection encryption upgrade and should use tls:// scheme instead.
 */
final class HandshakeHandler
{
    private string $scramble = '';
    private string $authPlugin = '';
    private int $serverCapabilities = 0;
    private int $sequenceId = 0;
    private bool $isSslEnabled = false;
    private Promise $promise;

    public function __construct(
        private readonly SocketConnection $socket,
        private readonly ConnectionParams $params
    ) {
        $this->promise = new Promise();
    }

    public function start(UncompressedPacketReader $packetReader): PromiseInterface
    {
        if ($packetReader->hasPacket()) {
            $packetReader->readPayload($this->handleInitialHandshake(...));
        }

        return $this->promise;
    }

    public function processPacket(PayloadReader $payloadReader, int $length, int $seq): void
    {
        if ($this->serverCapabilities === 0) {
            $this->handleInitialHandshake($payloadReader, $length, $seq);

            return;
        }

        $this->handleAuthResponse($payloadReader, $length, $seq);
    }

    private function handleInitialHandshake(PayloadReader $reader, int $length, int $seq): void
    {
        try {
            $parser = new HandshakeParser();
            /** @var \Rcalicdan\MySQLBinaryProtocol\Frame\Handshake\HandshakeV10 $handshake */
            $handshake = $parser->parse($reader, $length, $seq);

            $this->scramble = $handshake->authData;
            $this->authPlugin = $handshake->authPlugin;
            $this->serverCapabilities = $handshake->capabilities;

            $this->sequenceId = $seq + 1;

            $clientCaps = $this->calculateCapabilities();

            if ($this->params->useSsl() && ($this->serverCapabilities & CapabilityFlags::CLIENT_SSL)) {
                $this->performSslUpgrade($clientCaps);
            } else {
                if ($this->params->useSsl() && ! ($this->serverCapabilities & CapabilityFlags::CLIENT_SSL)) {
                    $this->promise->reject(new ConnectionException(
                        'SSL/TLS connection requested but server does not support SSL',
                        0
                    ));

                    return;
                }

                $this->sendAuthResponse($clientCaps);
            }
        } catch (\Throwable $e) {
            $wrappedException = new ConnectionException(
                'Failed to process initial handshake: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            );
            $this->promise->reject($wrappedException);
        }
    }

    private function performSslUpgrade(int $clientCaps): void
    {
        try {
            $writer = (new BufferPayloadWriterFactory())->create();
            $writer->writeUInt32($clientCaps | CapabilityFlags::CLIENT_SSL);
            $writer->writeUInt32(0x1000000);
            $writer->writeUInt8(CharsetIdentifiers::UTF8MB4);
            $writer->writeZeros(23);

            $payload = $writer->toString();
            $len = \strlen($payload);
            $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
            $packet = $header . $payload;

            $this->socket->write($packet);
            $this->sequenceId++;

            Loop::setImmediate(function () use ($clientCaps) {
                $this->configureSslAndEnable($clientCaps);
            });
        } catch (\Throwable $e) {
            $this->promise->reject(new ConnectionException(
                'Failed to initiate SSL upgrade: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            ));
        }
    }

    private function configureSslAndEnable(int $clientCaps): void
    {
        if (! method_exists($this->socket, 'enableEncryption')) {
            $this->promise->reject(new ConnectionException(
                'Socket does not support SSL/TLS upgrade. ' .
                    'MySQL requires STARTTLS capability for encrypted connections.'
            ));

            return;
        }

        try {
            $sslOptions = [
                'verify_peer' => $this->params->sslVerify,
                'verify_peer_name' => $this->params->sslVerify,
                'allow_self_signed' => ! $this->params->sslVerify,
                'crypto_method' => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT,
            ];

            if ($this->params->sslCa) {
                $sslOptions['cafile'] = $this->params->sslCa;
            }

            if ($this->params->sslCert) {
                $sslOptions['local_cert'] = $this->params->sslCert;
            }

            if ($this->params->sslKey) {
                $sslOptions['local_pk'] = $this->params->sslKey;
            }

            $this->socket->enableEncryption($sslOptions)->then(
                function () use ($clientCaps) {
                    $this->isSslEnabled = true;
                    $this->sendAuthResponse($clientCaps);
                },
                function ($e) {
                    $this->promise->reject(new ConnectionException(
                        'SSL/TLS handshake failed: ' . $e->getMessage(),
                        0,
                        $e
                    ));
                }
            );
        } catch (\Throwable $e) {
            $this->promise->reject(new ConnectionException(
                'Failed to configure SSL/TLS options: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            ));
        }
    }

    private function sendAuthResponse(int $clientCaps): void
    {
        try {
            $response = (new HandshakeResponse41())->build(
                $clientCaps,
                CharsetIdentifiers::UTF8MB4,
                $this->params->username,
                $this->generateAuthResponse($this->authPlugin, $this->scramble),
                $this->params->database,
                $this->authPlugin
            );

            $this->writePacket($response);
        } catch (\Throwable $e) {
            $this->promise->reject(new AuthenticationException(
                'Failed to build authentication response: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            ));
        }
    }

    private function handleAuthResponse(PayloadReader $reader, int $length, int $seq): void
    {
        $this->sequenceId = $seq + 1;
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === AuthPacketType::OK) {
            $this->handleAuthSuccess();

            return;
        }

        if ($firstByte === AuthPacketType::ERR) {
            $this->handleAuthError($reader);

            return;
        }

        if ($firstByte === AuthPacketType::AUTH_SWITCH_REQUEST) {
            $this->handleAuthPluginSwitch($reader);

            return;
        }

        if ($firstByte === AuthPacketType::AUTH_MORE_DATA) {
            $this->handleAuthMoreData($reader, $length);
        }
    }

    private function handleAuthSuccess(): void
    {
        $this->promise->resolve($this->sequenceId);
    }

    private function handleAuthError(PayloadReader $reader): void
    {
        $errorCode = $reader->readFixedInteger(2);
        $reader->readFixedString(1);
        $sqlState = $reader->readFixedString(5);
        $message = $reader->readRestOfPacketString();

        $exception = $this->createAuthException($errorCode, $sqlState, $message);
        $this->promise->reject($exception);
    }

    private function handleAuthPluginSwitch(PayloadReader $reader): void
    {
        try {
            $this->authPlugin = $reader->readNullTerminatedString();
            $this->scramble = $reader->readRestOfPacketString();
            $response = $this->generateAuthResponse($this->authPlugin, $this->scramble);
            $this->writePacket($response);
        } catch (\Throwable $e) {
            $this->promise->reject(new AuthenticationException(
                'Failed to handle auth plugin switch: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            ));
        }
    }

    private function handleAuthMoreData(PayloadReader $reader, int $length): void
    {
        try {
            if ($length <= 2) {
                $this->handleAuthSubStatus($reader);
            } else {
                $this->handleRsaPublicKey($reader);
            }
        } catch (\Throwable $e) {
            $this->promise->reject(new AuthenticationException(
                'Failed to handle authentication continuation: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            ));
        }
    }

    private function handleAuthSubStatus(PayloadReader $reader): void
    {
        $subStatus = $reader->readFixedInteger(1);

        if ($subStatus === AuthPacketType::FULL_AUTH_REQUIRED) {
            $this->sendFullAuthCredentials();
        } elseif ($subStatus === AuthPacketType::FAST_AUTH_SUCCESS) {
            // Fast auth success, waiting for final OK packet
            return;
        }
    }

    private function sendFullAuthCredentials(): void
    {
        if ($this->isSslEnabled) {
            // Send plaintext password over encrypted connection
            $this->writePacket($this->params->password . "\0");
        } else {
            // Request RSA public key for encryption
            $this->writePacket(\chr(0x02));
        }
    }

    private function handleRsaPublicKey(PayloadReader $reader): void
    {
        $publicKey = $reader->readRestOfPacketString();

        try {
            $encrypted = AuthScrambler::scrambleSha256Rsa(
                $this->params->password,
                $this->scramble,
                $publicKey
            );
            $this->writePacket($encrypted);
        } catch (\Throwable $e) {
            $this->promise->reject(new AuthenticationException(
                'Failed to encrypt password with RSA: ' . $e->getMessage(),
                (int)$e->getCode(),
                $e
            ));
        }
    }

    private function writePacket(string $payload): void
    {
        $len = \strlen($payload);
        $header = substr(pack('V', $len), 0, 3) . \chr($this->sequenceId);
        $this->socket->write($header . $payload);
        $this->sequenceId++;
    }

    private function generateAuthResponse(string $plugin, string $scramble): string
    {
        try {
            if ($plugin === 'mysql_native_password') {
                return AuthScrambler::scrambleNativePassword($this->params->password, $scramble);
            }

            if ($plugin === 'caching_sha2_password') {
                return AuthScrambler::scrambleCachingSha2Password($this->params->password, $scramble);
            }

            // Unknown plugin - return empty response
            return '';
        } catch (\Throwable $e) {
            throw new AuthenticationException(
                "Failed to generate authentication response for plugin '{$plugin}': " . $e->getMessage(),
                (int)$e->getCode(),
                $e
            );
        }
    }

    private function createAuthException(int $errorCode, string $sqlState, string $message): AuthenticationException
    {
        $authErrorCodes = [
            1045 => 'Access denied - Invalid username or password',
            1040 => 'Too many connections',
            1129 => 'Host is blocked due to too many connection errors',
            1130 => 'Host is not allowed to connect',
            1131 => 'Access denied - No permission to connect',
            1132 => 'Password change required',
            1133 => 'Password has expired',
            1227 => 'Access denied - Insufficient privileges',
            1251 => 'Client does not support authentication protocol',
            2049 => 'Connection using old (pre-4.1.1) authentication protocol refused',
        ];

        $errorDescription = $authErrorCodes[$errorCode] ?? 'Authentication failed';

        return new AuthenticationException(
            "MySQL Authentication Error [{$errorCode}] [{$sqlState}]: {$message} - {$errorDescription}",
            $errorCode
        );
    }

    private function calculateCapabilities(): int
    {
        $flags = CapabilityFlags::CLIENT_PROTOCOL_41 |
            CapabilityFlags::CLIENT_SECURE_CONNECTION |
            CapabilityFlags::CLIENT_LONG_PASSWORD |
            CapabilityFlags::CLIENT_TRANSACTIONS |
            CapabilityFlags::CLIENT_PLUGIN_AUTH |
            CapabilityFlags::CLIENT_MULTI_RESULTS |
            CapabilityFlags::CLIENT_PS_MULTI_RESULTS |
            CapabilityFlags::CLIENT_CONNECT_WITH_DB |
            CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA |
            CapabilityFlags::CLIENT_LOCAL_FILES;

        if ($this->params->useSsl()) {
            $flags |= CapabilityFlags::CLIENT_SSL;
        }

        return $flags;
    }
}
