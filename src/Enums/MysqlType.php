<?php

namespace Hibla\MysqlClient\Enums;

enum MysqlType: int
{
    case NULL = 0x06;

    case DOUBLE = 0x05;

    case LONGLONG = 0x08;
    
    case VAR_STRING = 0x0F;
}