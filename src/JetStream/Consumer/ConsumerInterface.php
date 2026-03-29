<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

use Sofascore\Nats\JetStream\Message\JetStreamMessage;

interface ConsumerInterface
{
    public function fetch(int $batch, ?FetchOptions $opts = null): MessageBatch;

    public function fetchBytes(int $maxBytes, ?FetchOptions $opts = null): MessageBatch;

    public function fetchNoWait(int $batch): MessageBatch;

    /**
     * Prepares a foreground callback loop.
     * Call run() on the returned context to start it.
     */
    public function consumeLoop(\Closure $handler, ?ConsumeLoopOptions $opts = null): ConsumeContext;

    /**
     * Creates a pull-oriented poller over repeated fetch() calls.
     */
    public function poll(?MessagePollOptions $opts = null): MessagePoller;

    public function next(float $timeout = 5.0): JetStreamMessage;

    public function info(): ConsumerInfo;

    public function cachedInfo(): ConsumerInfo;
}
