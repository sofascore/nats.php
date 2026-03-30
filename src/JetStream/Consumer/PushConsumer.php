<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

use Sofascore\Nats\JetStream\Error\PushConsumerException;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Message\JetStreamMsg;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\Subscription;

final class PushConsumer implements PushConsumerInterface
{
    private ?Subscription $subscription = null;
    private PushConsumerStatus $status = PushConsumerStatus::Unsubscribed;
    private ?PushConsumeContext $consumeContext = null;
    private ?PushConsumeOptions $consumeOptions = null;
    private ?\Closure $userHandler = null;
    private float $lastHeartbeatTime = 0.0;
    private float $heartbeatIntervalSeconds = 0.0;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $stream,
        private readonly ConsumerConfig $config,
        private ?ConsumerInfo $cachedInfo = null,
    ) {
        if ($this->config->deliverSubject === null) {
            throw new NatsException('Push consumer requires a deliver_subject in configuration');
        }
    }

    public function consume(\Closure $handler, ?PushConsumeOptions $opts = null): PushConsumeContext
    {
        if ($this->status !== PushConsumerStatus::Unsubscribed && $this->status !== PushConsumerStatus::Closed) {
            throw new NatsException('Consumer is already active; stop the current consumption first');
        }

        $opts ??= new PushConsumeOptions();
        $this->consumeOptions = $opts;
        $this->userHandler = $handler;
        $this->heartbeatIntervalSeconds = $this->config->idleHeartbeat ?? 0.0;
        $this->lastHeartbeatTime = microtime(true);

        $ctx = new PushConsumeContext($this);

        $wrappedHandler = function (Message $msg) use ($ctx): void {
            if ($ctx->isStopped()) {
                return;
            }
            $this->handleIncomingMessage($msg);
        };

        $deliverSubject = $this->config->deliverSubject;
        $deliverGroup = $this->config->deliverGroup;

        try {
            if ($deliverGroup !== null) {
                $this->subscription = $this->js->connection()->queueSubscribe(
                    $deliverSubject,
                    $deliverGroup,
                    $wrappedHandler,
                );
            } else {
                $this->subscription = $this->js->connection()->subscribe(
                    $deliverSubject,
                    $wrappedHandler,
                );
            }
        } catch (\Throwable $e) {
            $this->consumeOptions = null;
            $this->userHandler = null;
            throw $e;
        }

        $this->status = PushConsumerStatus::Active;
        $this->consumeContext = $ctx;

        return $ctx;
    }

    public function status(): PushConsumerStatus
    {
        return $this->status;
    }

    public function info(): ConsumerInfo
    {
        $consumerName = $this->config->name ?? $this->config->durable ?? '';
        $data = $this->js->apiRequest("CONSUMER.INFO.{$this->stream}.{$consumerName}");
        $this->cachedInfo = ConsumerInfo::fromArray($data);
        return $this->cachedInfo;
    }

    public function cachedInfo(): ?ConsumerInfo
    {
        return $this->cachedInfo;
    }

    public function name(): string
    {
        return $this->config->name ?? $this->config->durable ?? '';
    }

    public function deliverSubject(): string
    {
        return $this->config->deliverSubject ?? '';
    }

    public function unsubscribe(): void
    {
        $this->subscription?->unsubscribe();
        $this->subscription = null;
        $this->status = PushConsumerStatus::Closed;
        $this->consumeContext = null;
    }

    public function drain(): void
    {
        $this->subscription?->drain();
        $this->subscription = null;
        $this->status = PushConsumerStatus::Closed;
        $this->consumeContext = null;
    }

    public function checkHeartbeat(): void
    {
        if ($this->status !== PushConsumerStatus::Active && $this->status !== PushConsumerStatus::Stalled) {
            return;
        }
        $this->checkHeartbeatTimeout();
    }

    private function handleIncomingMessage(Message $msg): void
    {
        $this->checkHeartbeatTimeout();

        if ($msg->headers === null) {
            $this->deliverToUser($msg);
            return;
        }

        $statusRaw = $msg->headers->get('Status');
        if ($statusRaw === null) {
            $this->deliverToUser($msg);
            return;
        }

        $statusCode = (int) trim(explode(' ', $statusRaw)[0]);

        // Prefer the Description header (nats.go reads it separately),
        // fall back to parsing the suffix of the Status header value.
        $description = $msg->headers->get('Description');
        if ($description === null || $description === '') {
            $description = trim(substr($statusRaw, strlen((string) $statusCode))) ?: null;
        }

        match (true) {
            $statusCode === 100 => $this->handleStatus100($msg),
            $statusCode === 409 => $this->handleStatus409($description ?? 'Consumer error'),
            $statusCode >= 400 => $this->handleTerminalStatus($statusCode, $description ?? 'Server error'),
            default => $this->reportError(
                new PushConsumerException("Unexpected status {$statusCode}: {$description}", $statusCode),
            ),
        };
    }

    private function handleStatus100(Message $msg): void
    {
        if ($msg->replyTo !== null && $msg->replyTo !== '') {
            // Flow control: server expects an empty reply to resume delivery
            try {
                $this->js->connection()->publish($msg->replyTo);
            } catch (\Throwable $e) {
                $this->reportError($e);
            }
        }

        // Both flow-control and idle heartbeat reset the clock
        $this->lastHeartbeatTime = microtime(true);
    }

    private function handleStatus409(string $description): void
    {
        // "Consumer Deleted" is terminal — the consumer no longer exists server-side.
        // Other 409s (e.g. "Leadership Changed", "Exceeded MaxAckPending") are transient
        // and surfaced via the error handler without terminating the consumer.
        if (str_contains($description, 'Consumer Deleted')) {
            $this->handleTerminalStatus(409, $description);
            return;
        }

        $this->reportError(
            new PushConsumerException("Status 409: {$description}", statusCode: 409),
        );
    }

    private function handleTerminalStatus(int $statusCode, string $description): void
    {
        $this->status = PushConsumerStatus::Error;
        $this->reportError(
            new PushConsumerException("Terminal status {$statusCode}: {$description}", $statusCode),
        );
    }

    private function checkHeartbeatTimeout(): void
    {
        if ($this->heartbeatIntervalSeconds <= 0.0) {
            return;
        }

        // Pause heartbeat monitoring during reconnect — the transport is down,
        // not the consumer. Reset the clock so we don't fire a false stall
        // immediately after reconnection completes.
        if ($this->js->connection()->isReconnecting()) {
            $this->lastHeartbeatTime = microtime(true);
            return;
        }

        $elapsed = microtime(true) - $this->lastHeartbeatTime;
        $threshold = $this->heartbeatIntervalSeconds * 2.0;

        if ($elapsed > $threshold) {
            $this->status = PushConsumerStatus::Stalled;
            $this->reportError(
                new PushConsumerException(
                    sprintf('No heartbeat received for %.1fs (threshold: %.1fs)', $elapsed, $threshold),
                    statusCode: 100,
                ),
            );
            // Reset clock to avoid firing on every subsequent message
            $this->lastHeartbeatTime = microtime(true);
        }
    }

    private function deliverToUser(Message $msg): void
    {
        // Real messages reset the heartbeat clock — server is alive
        if ($this->heartbeatIntervalSeconds > 0.0) {
            $this->lastHeartbeatTime = microtime(true);
        }

        // Recover from stalled if we receive a real message
        if ($this->status === PushConsumerStatus::Stalled) {
            $this->status = PushConsumerStatus::Active;
        }

        $jsMsg = new JetStreamMsg($msg, $this->js->connection());
        if ($this->userHandler !== null) {
            ($this->userHandler)($jsMsg);
        }
    }

    private function reportError(\Throwable $e): void
    {
        $this->consumeContext?->setError($e);

        if ($this->consumeOptions?->errorHandler !== null) {
            ($this->consumeOptions->errorHandler)($e);
        }
    }
}
