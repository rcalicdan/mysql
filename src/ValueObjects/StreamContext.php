<?php

declare(strict_types=1);

namespace Hibla\Mysql\ValueObjects;

final class StreamContext
{
    /**
     * @param callable(array<string, mixed>): void $onRow Callback to handle each row
     * @param callable(StreamStats): void|null $onComplete Callback when streaming completes
     * @param callable(\Throwable): void|null $onError Callback for error handling
     */
    public function __construct(
        public readonly mixed $onRow,
        public readonly mixed $onComplete = null,
        public readonly mixed $onError = null,
    ) {
    }
}
