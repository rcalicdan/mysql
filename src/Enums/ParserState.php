<?php

namespace Hibla\MysqlClient\Enums;

enum ParserState: int
{
    case INIT = 0;

    case COLUMNS = 1;
    
    case ROWS = 2;
}