<?php

declare(strict_types=1);

namespace Nats\JetStream\Publish;

use Nats\Message;

final class PubAckFuture
{
    private ?PubAck $result = null;
    private ?\Throwable $error = null;
    private bool $complete = false;

    public function __construct(
        private readonly Message $originalMessage,
    ) {}

    public function ok(): PubAck
    {
        if ($this->result !== null) {
            return $this->result;
        }
        if ($this->error !== null) {
            throw $this->error;
        }
        throw new \LogicException('PubAckFuture is not complete; process the connection before calling ok()');
    }

    public function error(): ?\Throwable
    {
        return $this->error;
    }

    public function message(): Message
    {
        return $this->originalMessage;
    }

    public function isComplete(): bool
    {
        return $this->complete;
    }

    /** @internal */
    public function resolve(PubAck $ack): void
    {
        $this->result = $ack;
        $this->complete = true;
    }

    /** @internal */
    public function reject(\Throwable $error): void
    {
        $this->error = $error;
        $this->complete = true;
    }
}
