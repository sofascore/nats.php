<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

use Sofascore\Nats\JetStream\Message\JetStreamMessage;

interface PushConsumerInterface
{
    /**
     * Returns information about the push consumer.
     */
    public function info(): ConsumerInfo;

    /**
     * Returns the last cached consumer info without making a server request.
     */
    public function cachedInfo(): ConsumerInfo;

    /**
     * Returns the consumer name.
     */
    public function name(): string;

    /**
     * Returns the deliver subject this push consumer is bound to.
     */
    public function deliverSubject(): string;

    /**
     * Subscribe and consume messages via a callback.
     *
     * Flow-control messages are responded to automatically. Idle heartbeat
     * timeouts are detected and reported via the error handler. Terminal
     * status messages (e.g. 409) transition the consumer to Error state.
     *
     * @param \Closure(JetStreamMessage): void $handler
     */
    public function consume(\Closure $handler, ?PushConsumeOptions $opts = null): PushConsumeContext;

    /**
     * Returns the current lifecycle status of the push consumer.
     */
    public function status(): PushConsumerStatus;

    /**
     * Unsubscribes the push consumer from its deliver subject.
     */
    public function unsubscribe(): void;

    /**
     * Drains the push consumer subscription.
     */
    public function drain(): void;
}
