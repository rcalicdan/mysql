<?php

namespace Hibla\MysqlClient\Enums;

enum ConnectionState
{
    case DISCONNECTED;
    case HANDSHAKING;
    case AUTHENTICATING;
    case READY;
    case QUERYING;
    case CLOSING;
    case CLOSED;
}
