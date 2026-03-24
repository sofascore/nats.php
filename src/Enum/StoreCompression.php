<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum StoreCompression: string
{
    case None = 'none';
    case S2 = 's2';
}
