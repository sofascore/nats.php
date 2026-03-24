<?php

declare(strict_types=1);

namespace Sofascore\Nats\Enum;

enum RetentionPolicy: string
{
    case Limits = 'limits';
    case Interest = 'interest';
    case WorkQueue = 'workqueue';
}
