<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\TimeoutException;

/**
 * Pull-oriented iterator over repeated fetch() calls.
 *
 * @implements \IteratorAggregate<int, JetStreamMessage>
 */
final class MessagePoller implements \IteratorAggregate
{
    /** @var list<JetStreamMessage> */
    private array $buffer = [];
    private bool $stopped = false;
    private bool $draining = false;

    /** @internal */
    public function __construct(
        private readonly Consumer $consumer,
        private readonly MessagePollOptions $options,
    ) {}

    public function next(float $timeout = 30.0): JetStreamMessage
    {
        if ($this->buffer !== []) {
            return array_shift($this->buffer);
        }

        $batch = $this->consumer->fetch(
            $this->options->maxMessages,
            new FetchOptions(timeout: $timeout),
        );

        foreach ($batch->messages() as $msg) {
            $this->buffer[] = $msg;
        }

        if ($this->buffer !== []) {
            return array_shift($this->buffer);
        }

        throw new TimeoutException('No messages available');
    }

    /**
     * Returns the next message or null when no message arrives before the timeout.
     * When omitted, the timeout defaults to the poller expires setting.
     */
    public function tryNext(?float $timeout = null): ?JetStreamMessage
    {
        try {
            return $this->next($timeout ?? $this->options->expires);
        } catch (TimeoutException) {
            return null;
        }
    }

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

    /** @return \Generator<JetStreamMessage> */
    public function getIterator(): \Generator
    {
        while (!$this->stopped) {
            try {
                $msg = $this->next($this->options->expires);
                yield $msg;
            } catch (TimeoutException) {
                if ($this->draining) {
                    $this->stopped = true;
                    return;
                }
                continue;
            }
        }
    }

    /** @internal */
    public function addMessage(JetStreamMessage $msg): void
    {
        $this->buffer[] = $msg;
    }
}
