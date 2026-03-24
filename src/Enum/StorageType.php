<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum StorageType: string
{
    case File = 'file';
    case Memory = 'memory';
}
