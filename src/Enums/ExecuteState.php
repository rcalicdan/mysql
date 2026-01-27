<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Enums;

enum ExecuteState: int
{
    case HEADER = 0;

    case CHECK_DATA = 1;

    case ROWS = 2;
}
