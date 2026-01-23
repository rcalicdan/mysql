<?php

namespace Hibla\MysqlClient\Enums;

enum FieldType: int
{
    case TINY = 0x01;
    
    case SHORT = 0x02;

    case LONG = 0x03;

    case FLOAT = 0x04;

    case DOUBLE = 0x05;

    case TIMESTAMP = 0x07;

    case LONGLONG = 0x08;

    case MEDIUM = 0x09;

    case DATE = 0x0A;

    case TIME = 0x0B;

    case DATETIME = 0x0C;
    
    case VAR_STRING = 0x0F; 

    case STRING = 0xFE;        

    case BLOB = 0xFC;   
    
    case TINY_BLOB = 0xF9;

    case MEDIUM_BLOB = 0xFA;

    case LONG_BLOB = 0xFB;

    case DECIMAL = 0x00;

    case NEWDECIMAL = 0xF6;

    case YEAR = 0x0D;

    case ENUM = 0xF7;

    case SET = 0xF8;

    case JSON = 0xF5;       
}