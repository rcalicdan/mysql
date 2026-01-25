<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\ValueObjects;

final readonly class ConnectionParams
{
    public function __construct(
        public string $host,
        public int $port = 3306,
        public string $username = 'root',
        public string $password = '',
        public string $database = '',
        public string $charset = 'utf8mb4',
        public int $connectTimeout = 10,
        public bool $ssl = false,
        public ?string $sslCa = null,
        public ?string $sslCert = null,
        public ?string $sslKey = null,
        public bool $sslVerify = false,
    ) {
    }

    /**
     * Creates ConnectionParams from array configuration.
     *
     * @param array<string, mixed> $config
     */
    public static function fromArray(array $config): self
    {
        return new self(
            host: $config['host'] ?? throw new \InvalidArgumentException('Host is required'),
            port: $config['port'] ?? 3306,
            username: $config['username'] ?? 'root',
            password: $config['password'] ?? '',
            database: $config['database'] ?? '',
            charset: $config['charset'] ?? 'utf8mb4',
            connectTimeout: $config['connect_timeout'] ?? 10,
            ssl: $config['ssl'] ?? false,
            sslCa: $config['ssl_ca'] ?? null,
            sslCert: $config['ssl_cert'] ?? null,
            sslKey: $config['ssl_key'] ?? null,
            sslVerify: $config['ssl_verify'] ?? false,
        );
    }

    /**
     * Creates ConnectionParams from MySQL URI.
     *
     * Supports URIs like:
     * - mysql://user:pass@localhost:3306/database
     * - mysql://user:pass@localhost/database?ssl=true&ssl_verify=true
     * - user:pass@localhost:3306/database (scheme is optional)
     *
     * @param string $uri MySQL connection URI
     * @throws \InvalidArgumentException if URI is invalid
     */
    public static function fromUri(string $uri): self
    {
        if (!str_contains($uri, '://')) {
            $uri = 'mysql://' . $uri;
        }

        $parts = parse_url($uri);

        if ($parts === false || !isset($parts['host'])) {
            throw new \InvalidArgumentException('Invalid MySQL URI: ' . $uri);
        }

        if (isset($parts['scheme']) && $parts['scheme'] !== 'mysql') {
            throw new \InvalidArgumentException(
                'Invalid URI scheme "' . $parts['scheme'] . '", expected "mysql"'
            );
        }

        $query = [];
        if (isset($parts['query'])) {
            parse_str($parts['query'], $query);
        }

        return new self(
            host: $parts['host'],
            port: $parts['port'] ?? 3306,
            username: isset($parts['user']) ? rawurldecode($parts['user']) : 'root',
            password: isset($parts['pass']) ? rawurldecode($parts['pass']) : '',
            database: isset($parts['path']) ? rawurldecode(ltrim($parts['path'], '/')) : '',
            charset: $query['charset'] ?? 'utf8mb4',
            connectTimeout: isset($query['connect_timeout']) ? (int) $query['connect_timeout'] : 10,
            ssl: isset($query['ssl']) ? filter_var($query['ssl'], FILTER_VALIDATE_BOOLEAN) : false,
            sslCa: $query['ssl_ca'] ?? null,
            sslCert: $query['ssl_cert'] ?? null,
            sslKey: $query['ssl_key'] ?? null,
            sslVerify: isset($query['ssl_verify']) ? filter_var($query['ssl_verify'], FILTER_VALIDATE_BOOLEAN) : false,
        );
    }

    public function hasPassword(): bool
    {
        return $this->password !== '';
    }

    public function hasDatabase(): bool
    {
        return $this->database !== '';
    }

    public function useSsl(): bool
    {
        return $this->ssl;
    }

    /**
     * Builds a sanitized URI string (with password masked).
     * Useful for logging and error messages.
     */
    public function toSafeUri(): string
    {
        $uri = 'mysql://';
        
        if ($this->username !== 'root') {
            $uri .= rawurlencode($this->username);
            if ($this->password !== '') {
                $uri .= ':***';
            }
            $uri .= '@';
        }
        
        $uri .= $this->host;
        
        if ($this->port !== 3306) {
            $uri .= ':' . $this->port;
        }
        
        if ($this->database !== '') {
            $uri .= '/' . rawurlencode($this->database);
        }
        
        return $uri;
    }
}