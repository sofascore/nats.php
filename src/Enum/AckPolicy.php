<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum AckPolicy: string
{
    case Explicit = 'explicit';
    case All = 'all';
    case None = 'none';
}
