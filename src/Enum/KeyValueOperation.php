<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum KeyValueOperation: string
{
    case Put = 'PUT';
    case Delete = 'DEL';
    case Purge = 'PURGE';
}
