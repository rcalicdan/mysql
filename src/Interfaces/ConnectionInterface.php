<?php

namespace Hibla\MysqlClient\Interfaces;

use Hibla\MysqlClient\Enums\ConnectionState;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\MysqlClient\ValueObjects\Result;
use Hibla\MysqlClient\ValueObjects\OkPacket;

interface ConnectionInterface
{
    /**
     * Execute a SQL query
     * 
     * @return PromiseInterface<Result|OkPacket>
     */
    public function query(string $sql): PromiseInterface;
    
    /**
     * Prepare a statement
     * 
     * @return PromiseInterface<StatementInterface>
     */
    public function prepare(string $sql): PromiseInterface;
    
    /**
     * Close the connection
     * 
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface;
    
    /**
     * Ping the server
     * 
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface;
    
    /**
     * Check if connection is ready
     */
    public function isReady(): bool;
    
    /**
     * Get current connection state
     */
    public function getState(): ConnectionState;
}