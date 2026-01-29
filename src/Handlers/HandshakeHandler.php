<?php

declare(strict_types=1);

namespace Hibla\Mysql\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Mysql\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
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
    private Promise $promise;
    private int $sequenceId = 0;
    private string $scramble = '';
    private string $authPlugin = '';
    private int $serverCapabilities = 0;
    private bool $isSslEnabled = false;

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
                $this->sendAuthResponse($clientCaps);
            }
        } catch (\Throwable $e) {
            $this->promise->reject($e);
        }
    }

    private function performSslUpgrade(int $clientCaps): void
    {
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
    }

    private function configureSslAndEnable(int $clientCaps): void
    {
        // Check if socket supports encryption upgrade (MySQL STARTTLS requirement)
        if (! method_exists($this->socket, 'enableEncryption')) {
            $this->promise->reject(new \RuntimeException(
                'Socket does not support SSL/TLS upgrade. ' .
                    'MySQL requires STARTTLS capability for encrypted connections.'
            ));

            return;
        }

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
                $this->promise->reject(new \RuntimeException(
                    'SSL Handshake Failed: ' . $e->getMessage(),
                    0,
                    $e
                ));
            }
        );
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
            $this->promise->reject($e);
        }
    }

    private function handleAuthResponse(PayloadReader $reader, int $length, int $seq): void
    {
        $this->sequenceId = $seq + 1;
        $firstByte = $reader->readFixedInteger(1);

        if ($firstByte === AuthPacketType::OK) {
            $this->promise->resolve($this->sequenceId);

            return;
        }

        if ($firstByte === AuthPacketType::ERR) {
            $errorCode = $reader->readFixedInteger(2);
            $reader->readFixedString(1);
            $reader->readFixedString(5);
            $msg = $reader->readRestOfPacketString();
            $this->promise->reject(new \RuntimeException("MySQL Handshake Error [{$errorCode}]: {$msg}"));

            return;
        }

        if ($firstByte === AuthPacketType::AUTH_SWITCH_REQUEST) {
            $this->authPlugin = $reader->readNullTerminatedString();
            $this->scramble = $reader->readRestOfPacketString();
            $response = $this->generateAuthResponse($this->authPlugin, $this->scramble);
            $this->writePacket($response);

            return;
        }

        if ($firstByte === AuthPacketType::AUTH_MORE_DATA) {
            if ($length <= 2) {
                $subStatus = $reader->readFixedInteger(1);

                if ($subStatus === AuthPacketType::FULL_AUTH_REQUIRED) {
                    if ($this->isSslEnabled) {
                        $this->writePacket($this->params->password . "\0");
                    } else {
                        $this->writePacket(\chr(0x02));
                    }
                } elseif ($subStatus === AuthPacketType::FAST_AUTH_SUCCESS) {
                    // Fast auth success, waiting for final OK packet
                    return;
                }
            } else {
                $keyPart = $reader->readRestOfPacketString();

                try {
                    $encrypted = AuthScrambler::scrambleSha256Rsa(
                        $this->params->password,
                        $this->scramble,
                        $keyPart
                    );
                    $this->writePacket($encrypted);
                } catch (\Throwable $e) {
                    $this->promise->reject($e);
                }
            }
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
        if ($plugin === 'mysql_native_password') {
            return AuthScrambler::scrambleNativePassword($this->params->password, $scramble);
        }

        if ($plugin === 'caching_sha2_password') {
            return AuthScrambler::scrambleCachingSha2Password($this->params->password, $scramble);
        }

        return '';
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
