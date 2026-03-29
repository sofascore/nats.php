<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

readonly class MessagePollOptions
{
    public function __construct(
        public int $maxMessages = 100,
        public float $expires = 30.0,
    ) {}
}
