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
        
        $scheme = $this->params->useSsl() ? 'tls' : 'tcp';
        $uri = "{$scheme}://{$this->host}:{$this->port}";
        
        return $this->connector
            ->connect($uri)
            ->then(function ($stream) use ($connection) {
                $authPromise = $connection->waitForAuthentication();
                $connection->attachStream($stream);

                return $authPromise->then(fn() => $connection);
            });
    }

    public static function create(array $config): self
    {
        $params = ConnectionParams::fromArray($config);
        
        $connectorConfig = ['timeout' => $params->connectTimeout];
        
        if ($params->useSsl()) {
            $connectorConfig['tls'] = [
                'verify_peer' => $params->sslVerify,
                'verify_peer_name' => $params->sslVerify,
            ];
            
            if ($params->sslCa !== null) {
                $connectorConfig['tls']['cafile'] = $params->sslCa;
            }
            
            if ($params->sslCert !== null) {
                $connectorConfig['tls']['local_cert'] = $params->sslCert;
            }
            
            if ($params->sslKey !== null) {
                $connectorConfig['tls']['local_pk'] = $params->sslKey;
            }
        }
        
        $connector = new Connector($connectorConfig);
        
        $host = $config['host'] ?? 'localhost';
        $port = $config['port'] ?? 3306;
        
        return new self($connector, $params, $host, $port);
    }
}