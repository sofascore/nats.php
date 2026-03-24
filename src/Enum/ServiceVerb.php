<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum ServiceVerb: string
{
    case Ping = 'PING';
    case Stats = 'STATS';
    case Info = 'INFO';
}
