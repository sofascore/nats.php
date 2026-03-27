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
    private ?ConsumerInfo $cachedInfo = null;
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
        $this->status = PushConsumerStatus::Active;

        $ctx = new PushConsumeContext($this);
        $this->consumeContext = $ctx;

        $wrappedHandler = function (Message $msg) use ($ctx): void {
            if ($ctx->isStopped()) {
                return;
            }
            $this->handleIncomingMessage($msg);
        };

        $deliverSubject = $this->config->deliverSubject;
        $deliverGroup = $this->config->deliverGroup;

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

    public function cachedInfo(): ConsumerInfo
    {
        if ($this->cachedInfo === null) {
            return $this->info();
        }
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
        $description = trim(substr($statusRaw, strlen((string) $statusCode))) ?: null;

        match (true) {
            $statusCode === 100 => $this->handleStatus100($msg),
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
