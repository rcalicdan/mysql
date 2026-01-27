<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Enums;

enum PrepareState: int
{
    case HEADER = 0;

    case DRAIN_PARAMS = 1;

    case DRAIN_COLUMNS = 2;
}
