<?php

namespace Hibla\MysqlClient;

use Hibla\MysqlClient\Interfaces\ConnectionInterface;
use Hibla\MysqlClient\ValueObjects\ConnectionParams;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Socket\Connector;

final class Client
{
    public function __construct(
        private readonly Connector $connector,
        private readonly ConnectionParams $params,
        private readonly string $host,
        private readonly int $port = 3306
    ) {}

    /**
     * @return PromiseInterface<ConnectionInterface>
     */
    public function connect(): PromiseInterface
    {
        $connection = new Connection($this->params);
        
        return $this->connector
            ->connect("tcp://{$this->host}:{$this->port}")
            ->then(function ($stream) use ($connection) {
                $authPromise = $connection->waitForAuthentication();
                $connection->attachStream($stream);

                return $authPromise->then(fn() => $connection);
            });
    }

    public static function create(array $config): self
    {
        $params = ConnectionParams::fromArray($config);
        $connector = new Connector(['timeout' => $params->connectTimeout]);
        
        $host = $config['host'] ?? 'localhost';
        $port = $config['port'] ?? 3306;
        
        return new self($connector, $params, $host, $port);
    }
}