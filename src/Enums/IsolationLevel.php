<?php

declare(strict_types=1);

namespace Hibla\Mysql\Enums;

/**
 * Defines the standard transaction isolation levels for MySQL.
 */
enum IsolationLevel: string
{
    case READ_UNCOMMITTED = 'READ UNCOMMITTED';

    case READ_COMMITTED = 'READ COMMITTED';

    case REPEATABLE_READ = 'REPEATABLE READ';

    case SERIALIZABLE = 'SERIALIZABLE';
}
