<?php

declare(strict_types=1);

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Interfaces\ClientInterface;
use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\Interfaces\PreparedStatementInterface;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connector;

/**
 * MySQL Client - Main Entry Point
 * 
 * This is the primary class for interacting with MySQL databases.
 * It manages connections and provides high-level database operations.
 * 
 * @example Basic usage
 * ```php
 * $client = Client::create('localhost', 3306, [
 *     'username' => 'root',
 *     'password' => 'password',
 *     'database' => 'mydb',
 * ]);
 * 
 * await($client->query('SELECT * FROM users'));
 * ```
 */
class Client implements ClientInterface
{
    private ConnectionInterface $connection;

    private bool $autoConnect = true;

    private ?PromiseInterface $connectPromise = null;

    public function __construct(
        string $host,
        int $port,
        ConnectionParams $params,
        ?Connector $connector = null
    ) {
        $this->connection = new Connection($host, $port, $params, $connector);
    }

    /**
     * Create a new MySQL client instance.
     * 
     * @param array<string, mixed> $config Connection configuration including:
     *   - host: MySQL server hostname (required)
     *   - port: MySQL server port (default: 3306)
     *   - username: Database username (required)
     *   - password: Database password (default: '')
     *   - database: Database name (default: '')
     *   - charset: Character set (default: 'utf8mb4')
     *   - connect_timeout: Connection timeout in seconds (default: 10)
     *   - ssl: Enable SSL (default: true)
     *   - ssl_ca: SSL CA certificate path (optional)
     *   - ssl_cert: SSL certificate path (optional)
     *   - ssl_key: SSL key path (optional)
     *   - ssl_verify: Verify SSL certificate (default: true)
     * @param Connector|null $connector Custom socket connector (optional)
     * @return self
     * 
     * @example
     * ```php
     * $client = Client::create([
     *     'host' => 'localhost',
     *     'port' => 3306,
     *     'username' => 'root',
     *     'password' => 'password',
     *     'database' => 'mydb',
     * ]);
     * ```
     */
    public static function create(
        array $config,
        ?Connector $connector = null
    ): self {
        $host = $config['host'] ?? throw new \InvalidArgumentException('Host is required');
        $port = $config['port'] ?? 3306;

        $params = ConnectionParams::fromArray($config);

        return new self($host, $port, $params, $connector);
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->ensureConnected()
            ->then(fn() => $this->connection->query($sql));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql): PromiseInterface
    {
        return $this->ensureConnected()
            ->then(fn() => $this->connection->execute($sql));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): PromiseInterface
    {
        return Promise::rejected(
            new \RuntimeException('Prepared statements not yet implemented')
        );
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction(): PromiseInterface
    {
        return $this->execute('START TRANSACTION');
    }

    /**
     * {@inheritdoc}
     */
    public function commit(): PromiseInterface
    {
        return $this->execute('COMMIT');
    }

    /**
     * {@inheritdoc}
     */
    public function rollback(): PromiseInterface
    {
        return $this->execute('ROLLBACK');
    }

    /**
     * {@inheritdoc}
     */
    public function ping(): PromiseInterface
    {
        return $this->ensureConnected()
            ->then(fn() => $this->connection->ping());
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        $this->connection->close();
        $this->connectPromise = null;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }

    /**
     * Disable auto-connect behavior.
     * 
     * When disabled, you must manually call connect() before any operations.
     * 
     * @return self
     */
    public function withoutAutoConnect(): self
    {
        $this->autoConnect = false;

        return $this;
    }

    /**
     * Manually connect to the database.
     * 
     * @return PromiseInterface<ConnectionInterface>
     */
    public function connect(): PromiseInterface
    {
        if ($this->connectPromise !== null) {
            return $this->connectPromise;
        }

        $this->connectPromise = $this->connection->connect()
            ->catch(function (\Throwable $error) {
                $this->connectPromise = null;
                throw $error;
            });

        return $this->connectPromise;
    }

    /**
     * Ensure the connection is established before executing operations.
     */
    private function ensureConnected(): PromiseInterface
    {
        if ($this->connection->isReady()) {
            return Promise::resolved();
        }

        if (!$this->autoConnect) {
            return Promise::rejected(
                new \RuntimeException(
                    'Not connected. Call connect() first or enable auto-connect.'
                )
            );
        }

        return $this->connect()->then(fn() => null);
    }
}
