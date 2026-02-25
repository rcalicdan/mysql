<?php

declare(strict_types=1);

namespace Hibla\Mysql\Enums;

enum ConnectionState: string
{
    case DISCONNECTED = 'disconnected';

    case CONNECTING = 'connecting';

    case READY = 'ready';

    case QUERYING = 'querying';

    case CLOSED = 'closed';

    case PINGING = 'pinging';

    case PREPARING = 'preparing';

    case EXECUTING = 'executing';

    case RESETTING = 'resetting';
}
