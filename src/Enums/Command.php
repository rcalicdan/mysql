<?php

namespace Hibla\MysqlClient\Enums;

enum Command: string
{
    case QUIT = "\x01";

    case QUERY = "\x03";

    case STMT_PREPARE = "\x16";

    case STMT_EXECUTE = "\x17";
    
    case STMT_CLOSE = "\x19";
}