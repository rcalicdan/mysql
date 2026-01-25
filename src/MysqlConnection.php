<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\MysqlClient\Handlers\HandshakeHandler;
use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connector;
use Hibla\Socket\Interfaces\ConnectorInterface;
use Hibla\Socket\Interfaces\ConnectionInterface as SocketConnection;
use Rcalicdan\MySQLBinaryProtocol\Factory\DefaultPacketReaderFactory;
use Rcalicdan\MySQLBinaryProtocol\Packet\UncompressedPacketReader;

/**
 * MySQL connection that handles the complete connection lifecycle.
 */
class MysqlConnection implements ConnectionInterface
{
    /**
     * The normalized connection parameters.
     */
    private readonly ConnectionParams $params;

    private ConnectionState $state = ConnectionState::DISCONNECTED;

    private ?SocketConnection $socket = null;

    private ?UncompressedPacketReader $packetReader = null;

    private ?HandshakeHandler $handshakeHandler = null;

    private ?Promise $connectPromise = null;

    private bool $isClosingError = false;

    /**
     * @param ConnectionParams|array<string, mixed>|string $config Connection config object, array, or URI string.
     * @param ConnectorInterface|null $connector Optional custom connector.
     */
    public function __construct(
        ConnectionParams|array|string $config,
        private readonly ?ConnectorInterface $connector = null
    ) {
        $this->params = match (true) {
            $config instanceof ConnectionParams => $config,
            \is_array($config) => ConnectionParams::fromArray($config),
            \is_string($config) => ConnectionParams::fromUri($config),
        };
    }

    /**
     * Static factory to create and connect in one step.
     * 
     * @param ConnectionParams|array<string, mixed>|string $config
     * @return PromiseInterface<self>
     */
    public static function create(
        ConnectionParams|array|string $config,
        ?ConnectorInterface $connector = null
    ): PromiseInterface {
        $connection = new self($config, $connector);
        return $connection->connect();
    }

    /**
     * Establishes connection to MySQL server.
     * 
     * @return PromiseInterface<self>
     * @throws \LogicException if already connected or connecting
     */
    public function connect(): PromiseInterface
    {
        if ($this->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(
                new \LogicException('Connection is already active or connecting')
            );
        }

        $this->state = ConnectionState::CONNECTING;
        $this->connectPromise = new Promise();

        $connector = $this->connector ?? new Connector();

        $socketUri = \sprintf('tcp://%s:%d', $this->params->host, $this->params->port);

        $connector->connect($socketUri)->then(
            $this->handleSocketConnected(...),
            $this->handleConnectionError(...)
        );

        return $this->connectPromise;
    }

    /**
     * Called when TCP socket is successfully connected.
     * Initiates MySQL handshake protocol.
     */
    private function handleSocketConnected(SocketConnection $socket): void
    {
        if ($this->state !== ConnectionState::CONNECTING) {
            $socket->close();
            return;
        }

        $this->socket = $socket;
        $this->packetReader = (new DefaultPacketReaderFactory())->createWithDefaultSettings();
        $this->handshakeHandler = new HandshakeHandler($socket, $this->params);

        // Set up socket event handlers
        $this->socket->on('data', $this->handleData(...));
        $this->socket->on('close', $this->handleSocketClose(...));
        $this->socket->on('error', $this->handleSocketError(...));

        // Start MySQL handshake
        $this->handshakeHandler->start($this->packetReader)->then(
            $this->handleHandshakeSuccess(...),
            $this->handleHandshakeError(...)
        );
    }

    /**
     * Called when MySQL handshake completes successfully.
     */
    private function handleHandshakeSuccess(int $nextSeqId): void
    {
        if ($this->state !== ConnectionState::CONNECTING) {
            return;
        }

        $this->state = ConnectionState::READY;

        if ($this->connectPromise) {
            $this->connectPromise->resolve($this);
            $this->connectPromise = null;
        }
    }

    /**
     * Handles incoming data from the socket.
     * Processes MySQL protocol packets.
     */
    private function handleData(string $chunk): void
    {
        // Ignore data if connection is closed
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        if (!$this->packetReader) {
            return;
        }

        try {
            $this->packetReader->append($chunk);

            while ($this->packetReader->hasPacket()) {
                $this->packetReader->readPayload(function ($payloadReader, $length, $seq) {
                    if ($this->state === ConnectionState::CONNECTING && $this->handshakeHandler) {
                        // During handshake phase, delegate to handshake handler
                        $this->handshakeHandler->processPacket($payloadReader, $length, $seq);
                    } elseif ($this->state === ConnectionState::READY) {
                        // TODO: Phase 2 - Handle query/command responses
                    }
                });
            }
        } catch (\Throwable $e) {
            $this->handleError($e);
        }
    }

    /**
     * Handles TCP connection errors.
     */
    private function handleConnectionError(\Throwable $e): void
    {
        $this->handleError(new \RuntimeException(
            'Failed to establish TCP connection: ' . $e->getMessage(),
            0,
            $e
        ));
    }

    /**
     * Handles MySQL handshake errors (authentication, SSL, protocol errors).
     */
    private function handleHandshakeError(\Throwable $e): void
    {
        $this->handleError(new \RuntimeException(
            'MySQL handshake failed: ' . $e->getMessage(),
            0,
            $e
        ));
    }

    /**
     * Handles socket-level errors.
     */
    private function handleSocketError(\Throwable $e): void
    {
        $this->handleError(new \RuntimeException(
            'Socket error: ' . $e->getMessage(),
            0,
            $e
        ));
    }

    /**
     * Central error handler.
     * Ensures clean shutdown and proper promise rejection.
     */
    private function handleError(\Throwable $e): void
    {
        // Prevent re-entrancy
        if ($this->isClosingError) {
            return;
        }

        $this->isClosingError = true;
        $this->state = ConnectionState::CLOSED;

        // Capture promise before closing
        $promise = $this->connectPromise;
        $this->connectPromise = null;

        // Close socket if open
        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        // Clean up resources
        $this->packetReader = null;
        $this->handshakeHandler = null;

        // Reject promise with the actual error
        if ($promise) {
            $promise->reject($e);
        }

        $this->isClosingError = false;
    }

    /**
     * Handles socket close events.
     */
    private function handleSocketClose(): void
    {
        // If we're already handling an error, don't duplicate
        if ($this->isClosingError) {
            return;
        }

        $this->state = ConnectionState::CLOSED;

        // Clean up
        $this->socket = null;
        $this->packetReader = null;
        $this->handshakeHandler = null;

        // If we have a pending connection promise, reject it
        if ($this->connectPromise) {
            $this->connectPromise->reject(
                new \RuntimeException('Connection closed unexpectedly by server')
            );
            $this->connectPromise = null;
        }
    }

    /**
     * Gracefully closes the connection.
     */
    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        $this->state = ConnectionState::CLOSED;

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        $this->packetReader = null;
        $this->handshakeHandler = null;

        // Reject any pending connection promise
        if ($this->connectPromise) {
            $this->connectPromise->reject(
                new \RuntimeException('Connection closed by user')
            );
            $this->connectPromise = null;
        }
    }

    // ========================================================================
    // State queries
    // ========================================================================

    public function getState(): ConnectionState
    {
        return $this->state;
    }

    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    public function isClosed(): bool
    {
        return $this->state === ConnectionState::CLOSED;
    }

    // ========================================================================
    // Stubs for Phase 2 - Command execution
    // ========================================================================

    public function query(string $sql): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(new \RuntimeException('Connection not ready'));
        }

        return Promise::rejected(new \RuntimeException('Not implemented yet'));
    }

    public function execute(string $sql): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(new \RuntimeException('Connection not ready'));
        }

        return Promise::rejected(new \RuntimeException('Not implemented yet'));
    }

    public function ping(): PromiseInterface
    {
        if ($this->state !== ConnectionState::READY) {
            return Promise::rejected(new \RuntimeException('Connection not ready'));
        }

        return Promise::rejected(new \RuntimeException('Not implemented yet'));
    }
}
