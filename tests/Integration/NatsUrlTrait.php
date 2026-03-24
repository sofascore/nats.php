<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration;

trait NatsUrlTrait
{
    private static function natsUrl(): string
    {
        return getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';
    }
}
