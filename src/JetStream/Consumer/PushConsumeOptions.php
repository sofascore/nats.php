<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

readonly class PushConsumeOptions
{
    /**
     * @param \Closure(\Throwable): void|null $errorHandler
     */
    public function __construct(
        public ?\Closure $errorHandler = null,
    ) {}
}
