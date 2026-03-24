<?php

declare(strict_types=1);

namespace Sofascore\Nats;

final class TimeoutException extends NatsException
{
    public function __construct(string $message = 'Operation timed out')
    {
        parent::__construct($message);
    }
}
