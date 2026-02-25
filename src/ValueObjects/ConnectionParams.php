<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

final readonly class ConnectionParams
{
    public const float DEFAULT_KILL_TIMEOUT_SECONDS = 3.0;

    /**
     * @param string $host Hostname or IP of the MySQL server.
     * @param int $port TCP port (default 3306).
     * @param string $username MySQL username.
     * @param string $password MySQL password.
     * @param string $database Default schema to select on connect.
     * @param string $charset Connection character set.
     * @param int $connectTimeout Seconds before a connect attempt is aborted.
     * @param bool $ssl Whether to use SSL for the connection.
     * @param string|null $sslCa Path to the SSL CA certificate file.
     * @param string|null $sslCert Path to the SSL client certificate file.
     * @param string|null $sslKey Path to the SSL client key file.
     * @param bool $sslVerify Whether to verify the server SSL certificate.
     * @param float $killTimeoutSeconds How long to wait for a KILL QUERY side-channel
     *                                  to settle before proceeding with teardown.
     *                                  Must be greater than zero.
     * @param bool $enableServerSideCancellation Whether to dispatch KILL QUERY when a query
     *                                           promise is cancelled. When false, cancellation
     *                                           only transitions the promise state — the
     *                                           server-side query runs to completion. Defaults
     *                                           to true.
     * @param bool $compress Whether to enable MySQL Protocol Compression (zlib).
     *                       Requires the server to support CLIENT_COMPRESS.
     *                       Defaults to false.
     * @param bool $resetConnection Whether to send COM_RESET_CONNECTION before returning
     *                              the connection to the pool. Clears session state.
     *                              Defaults to false.
     */
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
        public float $killTimeoutSeconds = self::DEFAULT_KILL_TIMEOUT_SECONDS,
        public bool $enableServerSideCancellation = true,
        public bool $compress = false,
        public bool $resetConnection = false,
    ) {
        if ($this->killTimeoutSeconds <= 0) {
            throw new \InvalidArgumentException(
                \sprintf(
                    'killTimeoutSeconds must be greater than zero, %f given.',
                    $this->killTimeoutSeconds
                )
            );
        }
    }

    /**
     * Creates ConnectionParams from array configuration.
     *
     * Recognised keys:
     *   host, port, username, password, database, charset, connect_timeout,
     *   ssl, ssl_ca, ssl_cert, ssl_key, ssl_verify, kill_timeout_seconds,
     *   enable_server_side_cancellation, compress, reset_connection
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

        $killTimeoutSeconds = $config['kill_timeout_seconds'] ?? self::DEFAULT_KILL_TIMEOUT_SECONDS;
        $killTimeoutSeconds = is_numeric($killTimeoutSeconds)
            ? (float) $killTimeoutSeconds
            : self::DEFAULT_KILL_TIMEOUT_SECONDS;

        $enableServerSideCancellation = $config['enable_server_side_cancellation'] ?? true;
        $enableServerSideCancellation = \is_scalar($enableServerSideCancellation)
            ? (bool) $enableServerSideCancellation
            : true;

        $compress = $config['compress'] ?? false;
        $compress = \is_scalar($compress) ? (bool) $compress : false;

        $resetConnection = $config['reset_connection'] ?? false;
        $resetConnection = \is_scalar($resetConnection) ? (bool) $resetConnection : false;

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
            killTimeoutSeconds: $killTimeoutSeconds,
            enableServerSideCancellation: $enableServerSideCancellation,
            compress: $compress,
            resetConnection: $resetConnection,
        );
    }

    /**
     * Creates ConnectionParams from MySQL URI.
     *
     * Supports URIs like:
     * - mysql://user:pass@localhost:3306/database
     * - mysql://user:pass@localhost/database?ssl=true&ssl_verify=true
     * - mysql://user:pass@localhost/database?kill_timeout_seconds=5.0
     * - mysql://user:pass@localhost/database?enable_server_side_cancellation=false
     * - mysql://user:pass@localhost/database?compress=true&reset_connection=true
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

        $killTimeoutSeconds = isset($query['kill_timeout_seconds'])
            ? (float) $query['kill_timeout_seconds']
            : self::DEFAULT_KILL_TIMEOUT_SECONDS;

        $enableServerSideCancellation = isset($query['enable_server_side_cancellation'])
            ? filter_var($query['enable_server_side_cancellation'], FILTER_VALIDATE_BOOLEAN)
            : true;

        $compress = isset($query['compress'])
            ? filter_var($query['compress'], FILTER_VALIDATE_BOOLEAN)
            : false;

        $resetConnection = isset($query['reset_connection'])
            ? filter_var($query['reset_connection'], FILTER_VALIDATE_BOOLEAN)
            : false;

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
            killTimeoutSeconds: $killTimeoutSeconds,
            enableServerSideCancellation: $enableServerSideCancellation,
            compress: $compress,
            resetConnection: $resetConnection,
        );
    }

    /**
     * Returns a new instance with $enableServerSideCancellation overridden.
     *
     * Used by PoolManager to apply a pool-level cancellation override without
     * mutating the original ConnectionParams the caller passed in.
     */
    public function withQueryCancellation(bool $enabled): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            username: $this->username,
            password: $this->password,
            database: $this->database,
            charset: $this->charset,
            connectTimeout: $this->connectTimeout,
            ssl: $this->ssl,
            sslCa: $this->sslCa,
            sslCert: $this->sslCert,
            sslKey: $this->sslKey,
            sslVerify: $this->sslVerify,
            killTimeoutSeconds: $this->killTimeoutSeconds,
            enableServerSideCancellation: $enabled,
            compress: $this->compress,
            resetConnection: $this->resetConnection,
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
