<?php

namespace Hibla\MysqlClient\ValueObjects;

final readonly class ConnectionParams
{
    public function __construct(
        public string $username,
        public string $password = '',
        public string $database = '',
        public string $charset = 'utf8mb4',
        public int $connectTimeout = 10,
    ) {}

    public static function fromArray(array $config): self
    {
        return new self(
            username: $config['username'] ?? throw new \InvalidArgumentException('Username is required'),
            password: $config['password'] ?? '',
            database: $config['database'] ?? '',
            charset: $config['charset'] ?? 'utf8mb4',
            connectTimeout: $config['connect_timeout'] ?? 10,
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
}
