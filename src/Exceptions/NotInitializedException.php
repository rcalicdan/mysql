<?php

declare(strict_types=1);

namespace Hibla\Mysql\Exceptions;

use RuntimeException;

/**
 * Exception thrown when attempting to use a MysqlClient that hasn't been initialized.
 */
class NotInitializedException extends RuntimeException
{
}
