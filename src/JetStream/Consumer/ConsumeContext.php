<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

final class ConsumeContext
{
    private bool $stopped = false;
    private bool $draining = false;
    private ?\Throwable $lastError = null;

    /** @internal */
    public function __construct(
        private readonly ConsumerInterface $consumer,
        private readonly \Closure $handler,
        private readonly ConsumeLoopOptions $options,
    ) {}

    public function stop(): void
    {
        $this->stopped = true;
    }

    public function drain(): void
    {
        $this->draining = true;
    }

    public function isClosed(): bool
    {
        return $this->stopped;
    }

    /**
     * Returns the last error recorded while running the foreground consume loop.
     */
    public function lastError(): ?\Throwable
    {
        return $this->lastError;
    }

    /**
     * Returns current consumer info from the server.
     */
    public function info(): ConsumerInfo
    {
        return $this->consumer->info();
    }

    /**
     * Starts the foreground consume loop.
     *
     * The loop blocks until stop() or drain() ends it.
     */
    public function run(): void
    {
        while (!$this->stopped) {
            try {
                $batch = $this->consumer->fetch(
                    $this->options->maxMessages,
                    new FetchOptions(timeout: $this->options->expires),
                );

                foreach ($batch as $msg) {
                    if ($this->stopped) {
                        return;
                    }
                    ($this->handler)($msg);
                }

                if ($this->draining) {
                    $this->stopped = true;
                    return;
                }
            } catch (\Throwable $e) {
                $this->lastError = $e;

                if ($this->options->errorHandler !== null) {
                    ($this->options->errorHandler)($e);
                }

                if ($this->draining || $this->stopped) {
                    return;
                }
                // Brief pause before retry
                usleep(100_000);
            }
        }
    }
}
