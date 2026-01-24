<?php 


namespace Hibla\MysqlClient\Interfaces;

use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\MysqlClient\ValueObjects\Result;
use Hibla\MysqlClient\ValueObjects\OkPacket;

interface ConnectionInterface
{
    public function query(string $sql): PromiseInterface; 
    
    public function prepare(string $sql): PromiseInterface; 
    
    public function close(): PromiseInterface;
    
    public function ping(): PromiseInterface; 
}