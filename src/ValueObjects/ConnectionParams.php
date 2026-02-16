<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

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
        $host = $config['host'] ?? throw new \InvalidArgumentException('Host is required');
        if (! \is_string($host)) {
            throw new \InvalidArgumentException('Host must be a string');
        }

        $port = $config['port'] ?? 3306;
        $port = is_numeric($port) ? (int) $port : 3306;

        $username = $config['username'] ?? 'root';
        $username = \is_scalar($username) ? (string) $username : 'root';

        $password = $config['password'] ?? '';
        $password = \is_scalar($password) ? (string) $password : '';

        $database = $config['database'] ?? '';
        $database = \is_scalar($database) ? (string) $database : '';

        $charset = $config['charset'] ?? 'utf8mb4';
        $charset = \is_scalar($charset) ? (string) $charset : 'utf8mb4';

        $connectTimeout = $config['connect_timeout'] ?? 10;
        $connectTimeout = is_numeric($connectTimeout) ? (int) $connectTimeout : 10;

        $ssl = $config['ssl'] ?? false;
        $ssl = \is_scalar($ssl) ? (bool) $ssl : false;

        $sslCa = $config['ssl_ca'] ?? null;
        if (! \is_string($sslCa)) {
            $sslCa = null;
        }

        $sslCert = $config['ssl_cert'] ?? null;
        if (! \is_string($sslCert)) {
            $sslCert = null;
        }

        $sslKey = $config['ssl_key'] ?? null;
        if (! \is_string($sslKey)) {
            $sslKey = null;
        }

        $sslVerify = $config['ssl_verify'] ?? false;
        $sslVerify = \is_scalar($sslVerify) ? (bool) $sslVerify : false;

        return new self(
            host: $host,
            port: $port,
            username: $username,
            password: $password,
            database: $database,
            charset: $charset,
            connectTimeout: $connectTimeout,
            ssl: $ssl,
            sslCa: $sslCa,
            sslCert: $sslCert,
            sslKey: $sslKey,
            sslVerify: $sslVerify,
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
        if (! str_contains($uri, '://')) {
            $uri = 'mysql://' . $uri;
        }

        $parts = parse_url($uri);

        if ($parts === false || ! isset($parts['host'])) {
            throw new \InvalidArgumentException('Invalid MySQL URI: ' . $uri);
        }

        if (isset($parts['scheme']) && $parts['scheme'] !== 'mysql') {
            throw new \InvalidArgumentException(
                'Invalid URI scheme "' . $parts['scheme'] . '", expected "mysql"'
            );
        }

        $query = [];
        if (isset($parts['query']) && \is_string($parts['query'])) {
            parse_str($parts['query'], $query);
        }

        /** @var string|null $sslCa */
        $sslCa = isset($query['ssl_ca']) && \is_string($query['ssl_ca']) ? $query['ssl_ca'] : null;

        /** @var string|null $sslCert */
        $sslCert = isset($query['ssl_cert']) && \is_string($query['ssl_cert']) ? $query['ssl_cert'] : null;

        /** @var string|null $sslKey */
        $sslKey = isset($query['ssl_key']) && \is_string($query['ssl_key']) ? $query['ssl_key'] : null;

        return new self(
            host: (string) $parts['host'],
            port: isset($parts['port']) ? (int) $parts['port'] : 3306,
            username: isset($parts['user']) ? rawurldecode((string) $parts['user']) : 'root',
            password: isset($parts['pass']) ? rawurldecode((string) $parts['pass']) : '',
            database: isset($parts['path']) ? rawurldecode(ltrim((string) $parts['path'], '/')) : '',
            charset: isset($query['charset']) && \is_string($query['charset']) ? $query['charset'] : 'utf8mb4',
            connectTimeout: isset($query['connect_timeout']) ? (int) $query['connect_timeout'] : 10,
            ssl: isset($query['ssl']) ? filter_var($query['ssl'], FILTER_VALIDATE_BOOLEAN) : false,
            sslCa: $sslCa,
            sslCert: $sslCert,
            sslKey: $sslKey,
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
