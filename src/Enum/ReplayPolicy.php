<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum ReplayPolicy: string
{
    case Instant = 'instant';
    case Original = 'original';
}
