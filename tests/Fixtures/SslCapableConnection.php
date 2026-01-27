<?php

declare(strict_types=1);

namespace Tests\Fixtures;

use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Socket\Interfaces\ConnectionInterface;

interface SslCapableConnection extends ConnectionInterface
{
    public function enableEncryption(array $options): PromiseInterface;
}
