<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

final class PushConsumeContext
{
    private bool $stopped = false;
    private ?\Throwable $lastError = null;

    /** @internal */
    public function __construct(
        private readonly PushConsumer $consumer,
    ) {}

    public function stop(): void
    {
        $this->stopped = true;
        $this->consumer->unsubscribe();
    }

    public function isStopped(): bool
    {
        return $this->stopped;
    }

    public function lastError(): ?\Throwable
    {
        return $this->lastError;
    }

    public function info(): ConsumerInfo
    {
        return $this->consumer->info();
    }

    /** @internal */
    public function setError(\Throwable $e): void
    {
        $this->lastError = $e;
    }
}
