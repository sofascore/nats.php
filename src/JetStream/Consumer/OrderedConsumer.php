<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

use Sofascore\Nats\Enum\AckPolicy;
use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\Inbox;
use Sofascore\Nats\Internal\TypeCast as T;
use Sofascore\Nats\JetStream\Error\JetStreamException;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\JetStream\Message\MessageMetadata;
use Sofascore\Nats\JetStream\Message\JetStreamMsg;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\TimeoutException;

final class OrderedConsumer implements ConsumerInterface
{
    private ?Consumer $inner = null;
    private int $streamSequence = 0;
    private int $consumerSequence = 0;
    /** @var array<string, int> */
    private array $lastSubjectSequences = [];

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $stream,
        private readonly OrderedConsumerConfig $config,
    ) {}

    public function fetch(int $batch, ?FetchOptions $opts = null): MessageBatch
    {
        $opts ??= new FetchOptions();

        return $this->runPullLoop(
            limit: $batch,
            maxBytes: null,
            opts: $opts,
            noWait: false,
        );
    }

    public function fetchBytes(int $maxBytes, ?FetchOptions $opts = null): MessageBatch
    {
        $opts ??= new FetchOptions();

        return $this->runPullLoop(
            limit: PHP_INT_MAX,
            maxBytes: $maxBytes,
            opts: $opts,
            noWait: false,
        );
    }

    public function fetchNoWait(int $batch): MessageBatch
    {
        return $this->runPullLoop(
            limit: $batch,
            maxBytes: null,
            opts: new FetchOptions(timeout: 1.0),
            noWait: true,
        );
    }

    public function consumeLoop(\Closure $handler, ?ConsumeLoopOptions $opts = null): ConsumeContext
    {
        $opts ??= new ConsumeLoopOptions();

        return new ConsumeContext($this, $handler, $opts);
    }

    public function poll(?MessagePollOptions $opts = null): MessagePoller
    {
        $opts ??= new MessagePollOptions();

        return new MessagePoller($this, $opts);
    }

    public function next(float $timeout = 5.0): JetStreamMessage
    {
        $batch = $this->fetch(1, new FetchOptions(timeout: $timeout));
        foreach ($batch->messages() as $message) {
            return $message;
        }

        throw new TimeoutException('No message available');
    }

    public function info(): ConsumerInfo
    {
        try {
            return $this->getInner()->info();
        } catch (JetStreamException $error) {
            if (!$this->isMissingConsumerError($error)) {
                throw $error;
            }

            $this->recreateConsumer($this->streamSequence);

            return $this->getInner()->info();
        }
    }

    public function cachedInfo(): ConsumerInfo
    {
        return $this->getInner()->cachedInfo();
    }

    private function getInner(): Consumer
    {
        if ($this->inner === null) {
            $this->inner = $this->createOrderedConsumer($this->streamSequence);
            $this->consumerSequence = 0;
        }

        return $this->inner;
    }

    private function createOrderedConsumer(int $resumeStreamSequence): Consumer
    {
        $filterSubject = $this->config->filterSubject;
        $filterSubjects = $this->config->filterSubjects;
        if (
            $this->isLastPerSubject()
            && $filterSubject === null
            && $filterSubjects === []
        ) {
            $filterSubjects = ['>'];
        }

        $deliverPolicy = $this->resolveDeliverPolicy($resumeStreamSequence);

        $consumerConfig = new ConsumerConfig(
            name: Inbox::nuid(),
            deliverPolicy: $deliverPolicy,
            optStartSeq: $this->resolveOptStartSeq($resumeStreamSequence, $deliverPolicy),
            optStartTime: $this->resolveOptStartTime($resumeStreamSequence, $deliverPolicy),
            ackPolicy: AckPolicy::None,
            replicas: 1,
            headersOnly: $this->config->headersOnly,
            filterSubject: $filterSubject,
            filterSubjects: $filterSubjects,
            replayPolicy: $this->config->replayPolicy,
            memoryStorage: true,
            inactiveThreshold: $this->config->inactiveThreshold,
        );

        $data = $this->js->apiRequest(
            "CONSUMER.CREATE.{$this->stream}",
            ['stream_name' => $this->stream, 'config' => $consumerConfig->toArray()],
        );

        return new Consumer(
            $this->js,
            $this->stream,
            ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])),
        );
    }

    private function resolveDeliverPolicy(int $resumeStreamSequence): DeliveryPolicy
    {
        if ($resumeStreamSequence > 0 && !$this->isLastPerSubject()) {
            return DeliveryPolicy::ByStartSequence;
        }

        return $this->config->deliverPolicy;
    }

    private function resolveOptStartSeq(int $resumeStreamSequence, DeliveryPolicy $deliverPolicy): ?int
    {
        if ($resumeStreamSequence > 0 && $deliverPolicy === DeliveryPolicy::ByStartSequence) {
            return $resumeStreamSequence + 1;
        }

        return $deliverPolicy === DeliveryPolicy::ByStartSequence
            ? $this->config->optStartSeq
            : null;
    }

    private function resolveOptStartTime(int $resumeStreamSequence, DeliveryPolicy $deliverPolicy): ?\DateTimeImmutable
    {
        if ($resumeStreamSequence > 0) {
            return null;
        }

        return $deliverPolicy === DeliveryPolicy::ByStartTime
            ? $this->config->optStartTime
            : null;
    }

    private function runPullLoop(int $limit, ?int $maxBytes, FetchOptions $opts, bool $noWait): MessageBatch
    {
        $batch = new MessageBatch();
        $deadline = microtime(true) + $opts->timeout;
        $received = 0;
        $totalBytes = 0;
        $tentativeStreamSequence = $this->streamSequence;
        $tentativeConsumerSequence = $this->inner === null ? 0 : $this->consumerSequence;
        $tentativeSubjectSequences = $this->lastSubjectSequences;

        while ($received < $limit && ($maxBytes === null || $totalBytes < $maxBytes)) {
            $remainingTimeout = $noWait
                ? 1.0
                : max(0.001, $deadline - microtime(true));

            if (!$noWait && $remainingTimeout <= 0.001 && $received > 0) {
                break;
            }

            $pullResult = $this->pull(
                batch: $limit - $received,
                maxBytes: $maxBytes === null ? null : max(1, $maxBytes - $totalBytes),
                opts: new FetchOptions(
                    timeout: $remainingTimeout,
                    heartbeat: $opts->heartbeat,
                    minPending: $opts->minPending,
                    minAckPending: $opts->minAckPending,
                ),
                noWait: $noWait,
            );

            $needsReset = false;
            foreach ($pullResult['messages'] as $message) {
                $metadata = $message->metadata();
                if ($metadata->consumerSequence !== $tentativeConsumerSequence + 1) {
                    $needsReset = true;
                    break;
                }

                $tentativeConsumerSequence = $metadata->consumerSequence;
                if ($this->shouldSkipMessage($message, $metadata, $tentativeSubjectSequences)) {
                    continue;
                }

                $batch->addMessage($message);
                $received++;
                $totalBytes += strlen($message->data());
                $tentativeStreamSequence = $metadata->streamSequence;
                if ($this->isLastPerSubject()) {
                    $tentativeSubjectSequences[$message->subject()] = $metadata->streamSequence;
                }

                if ($received >= $limit || ($maxBytes !== null && $totalBytes >= $maxBytes)) {
                    break;
                }
            }

            if ($received >= $limit || ($maxBytes !== null && $totalBytes >= $maxBytes)) {
                break;
            }

            if ($needsReset || $this->shouldRecover($pullResult['status'])) {
                $this->recover($deadline, $tentativeStreamSequence);
                $tentativeConsumerSequence = 0;
                continue;
            }

            if (
                $received === 0
                && $pullResult['messages'] === []
                && !$this->currentConsumerExists()
            ) {
                $this->recover($deadline, $tentativeStreamSequence);
                $tentativeConsumerSequence = 0;
                continue;
            }

            if ($pullResult['status'] !== null || $pullResult['messages'] === []) {
                break;
            }
        }

        $this->streamSequence = $tentativeStreamSequence;
        $this->consumerSequence = $tentativeConsumerSequence;
        $this->lastSubjectSequences = $tentativeSubjectSequences;
        $batch->setComplete();

        return $batch;
    }

    /**
     * @return array{
     *     messages: list<JetStreamMessage>,
     *     status: null|array{code: int, description: string}
     * }
     */
    private function pull(int $batch, ?int $maxBytes, FetchOptions $opts, bool $noWait): array
    {
        $inbox = Inbox::generate();
        $sub = $this->js->connection()->subscribeSync($inbox);

        try {
            $requestData = [];
            if ($maxBytes !== null) {
                $requestData['max_bytes'] = $maxBytes;
            } else {
                $requestData['batch'] = $batch;
                if ($opts->minPending !== null) {
                    $requestData['min_pending'] = $opts->minPending;
                }
                if ($opts->minAckPending !== null) {
                    $requestData['min_ack_pending'] = $opts->minAckPending;
                }
            }

            if ($noWait) {
                $requestData['no_wait'] = true;
            } else {
                $requestData['expires'] = (int) ($opts->timeout * 1_000_000_000);
                if ($opts->heartbeat !== null) {
                    $requestData['idle_heartbeat'] = (int) ($opts->heartbeat * 1_000_000_000);
                }
            }

            $consumer = $this->getInner();
            $this->js->connection()->publish(
                "\$JS.API.CONSUMER.MSG.NEXT.{$this->stream}.{$consumer->name()}",
                json_encode($requestData, JSON_THROW_ON_ERROR),
                $inbox,
            );

            $messages = [];
            $status = null;
            $received = 0;
            $totalBytes = 0;
            $deadline = microtime(true) + $opts->timeout + 1.0;

            while ($received < $batch && ($maxBytes === null || $totalBytes < $maxBytes) && microtime(true) < $deadline) {
                try {
                    $message = $sub->nextMessage(max(0.001, $deadline - microtime(true)));
                } catch (TimeoutException) {
                    break;
                }

                if ($this->isStatusMessage($message)) {
                    $status = $this->extractStatus($message);
                    if ($status['code'] === 100) {
                        $status = null;
                        continue;
                    }
                    break;
                }

                $jsMessage = new JetStreamMsg($message, $this->js->connection());
                $messages[] = $jsMessage;
                $received++;
                $totalBytes += strlen($message->data);
            }

            return [
                'messages' => $messages,
                'status' => $status,
            ];
        } finally {
            $sub->unsubscribe();
        }
    }

    private function isStatusMessage(Message $message): bool
    {
        if ($message->headers === null) {
            return false;
        }

        if ($message->data !== '') {
            return false;
        }

        return $message->headers->get('Status') !== null;
    }

    /**
     * @return array{code: int, description: string}
     */
    private function extractStatus(Message $message): array
    {
        $statusRaw = trim((string) $message->headers?->get('Status'));
        $description = trim((string) ($message->headers?->get('Description') ?? ''));
        $statusCode = (int) explode(' ', $statusRaw)[0];

        if ($description === '') {
            $description = trim(substr($statusRaw, strlen((string) $statusCode)));
        }

        return [
            'code' => $statusCode,
            'description' => $description,
        ];
    }

    /**
     * @param array<string, int> $subjectSequences
     */
    private function shouldSkipMessage(
        JetStreamMessage $message,
        MessageMetadata $metadata,
        array $subjectSequences,
    ): bool {
        if (!$this->isLastPerSubject()) {
            return false;
        }

        return $metadata->streamSequence <= ($subjectSequences[$message->subject()] ?? 0);
    }

    private function isLastPerSubject(): bool
    {
        return $this->config->deliverPolicy === DeliveryPolicy::LastPerSubject;
    }

    /**
     * @param array{code: int, description: string}|null $status
     */
    private function shouldRecover(?array $status): bool
    {
        if ($status === null) {
            return false;
        }

        $description = strtolower($status['description']);

        if ($status['code'] === 503) {
            return true;
        }

        if ($status['code'] !== 409) {
            return false;
        }

        if (str_contains($description, 'consumer deleted')) {
            return true;
        }

        if (str_contains($description, 'leadership change')) {
            return true;
        }

        return str_contains($description, 'server shutdown');
    }

    private function currentConsumerExists(): bool
    {
        if ($this->inner === null) {
            return false;
        }

        try {
            $this->inner->info();
            return true;
        } catch (JetStreamException) {
            return false;
        }
    }

    private function isMissingConsumerError(JetStreamException $error): bool
    {
        $apiError = $error->apiError;
        if ($apiError !== null && $apiError->errCode === 10014) {
            return true;
        }

        $description = strtolower($apiError !== null ? $apiError->description : $error->getMessage());
        $code = $apiError !== null ? $apiError->code : $error->getCode();

        return $code === 404
            && str_contains($description, 'consumer')
            && str_contains($description, 'not found');
    }

    private function recover(float $deadline, int $resumeStreamSequence): void
    {
        $attempts = 0;
        $maxResetAttempts = $this->config->maxResetAttempts;
        $lastError = null;

        while ($maxResetAttempts === null || $attempts < $maxResetAttempts) {
            $attempts++;

            try {
                $this->recreateConsumer($resumeStreamSequence);
                return;
            } catch (\Throwable $error) {
                $lastError = $error;

                if (microtime(true) >= $deadline) {
                    break;
                }

                usleep(min(50_000 * $attempts, 250_000));
            }
        }

        if ($maxResetAttempts !== null && $attempts >= $maxResetAttempts) {
            throw new NatsException(
                sprintf('Failed to recreate ordered consumer after %d attempt(s)', $attempts),
                0,
                $lastError,
            );
        }

        throw new NatsException('Failed to recreate ordered consumer before the pull deadline elapsed', 0, $lastError);
    }

    private function recreateConsumer(int $resumeStreamSequence): void
    {
        $current = $this->inner;
        $this->inner = null;

        if ($current !== null) {
            try {
                $this->js->deleteConsumer($this->stream, $current->name());
            } catch (\Throwable) {
            }
        }

        $this->inner = $this->createOrderedConsumer($resumeStreamSequence);
        $this->consumerSequence = 0;
    }
}
