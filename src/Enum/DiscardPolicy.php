<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum DiscardPolicy: string
{
    case Old = 'old';
    case New = 'new';
}
