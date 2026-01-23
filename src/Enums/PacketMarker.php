<?php

namespace Hibla\MysqlClient\Enums;

enum PacketMarker: int
{
    case EOF = 0xFE;

    case OK = 0x00;
    
    case ERR = 0xFF;
}