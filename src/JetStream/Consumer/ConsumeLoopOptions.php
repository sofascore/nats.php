<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

readonly class ConsumeLoopOptions
{
    /**
     * @param \Closure(\Throwable): void|null $errorHandler
     */
    public function __construct(
        public int $maxMessages = 100,
        public float $expires = 30.0,
        public ?\Closure $errorHandler = null,
    ) {}
}
