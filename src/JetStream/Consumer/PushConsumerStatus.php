<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

enum PushConsumerStatus: string
{
    case Unsubscribed = 'unsubscribed';
    case Active = 'active';
    case Stalled = 'stalled';
    case Error = 'error';
    case Closed = 'closed';
}
