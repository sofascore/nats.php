<?php

declare(strict_types=1);

namespace Sofascore\Nats;

final class ConnectionClosedException extends NatsException
{
    public function __construct()
    {
        parent::__construct('Connection is closed');
    }
}
