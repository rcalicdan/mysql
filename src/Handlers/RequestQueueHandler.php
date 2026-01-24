<?php

namespace Hibla\MysqlClient\Handlers;

use Hibla\Promise\Promise;

final class RequestQueueHandler
{
    private array $pending = [];
    
    public function enqueue(): Promise
    {
        $promise = new Promise();
        $this->pending[] = $promise;
        
        return $promise;
    }
    
    public function resolve(mixed $value): void
    {
        if (empty($this->pending)) {
            return;
        }
        
        $promise = array_shift($this->pending);
        $promise->resolve($value);
    }
    
    public function reject(\Throwable $error): void
    {
        if (empty($this->pending)) {
            return;
        }
        
        $promise = array_shift($this->pending);
        $promise->reject($error);
    }
    
    public function rejectAll(\Throwable $error): void
    {
        foreach ($this->pending as $promise) {
            $promise->reject($error);
        }
        
        $this->pending = [];
    }
    
    public function isEmpty(): bool
    {
        return empty($this->pending);
    }
}