<?php

declare(strict_types=1);

namespace Hibla\MysqlClient\Enums;

enum ConnectionState: string
{
    case DISCONNECTED = 'disconnected';

    case CONNECTING = 'connecting';

    case HANDSHAKING = 'handshaking';

    case AUTHENTICATING = 'authenticating';

    case READY = 'ready';

    case QUERYING = 'querying';

    case CLOSING = 'closing';

    case CLOSED = 'closed';

    case PINGING = 'pinging';

    case PREPARING = 'preparing';

    case EXECUTING = 'executing';
}
