<?php

declare(strict_types=1);

namespace Sofascore\Nats\KeyValue;

use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\Enum\KeyValueOperation;
use Sofascore\Nats\Headers;
use Sofascore\Nats\JetStream\Consumer\FetchOptions;
use Sofascore\Nats\JetStream\Consumer\OrderedConsumerConfig;
use Sofascore\Nats\JetStream\Error\JetStreamException;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\JetStream\Publish\PublishOptions;
use Sofascore\Nats\JetStream\Stream\StreamPurgeOptions;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;

final class KeyValue implements KeyValueInterface
{
    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $bucketName,
    ) {}

    /**
     * @throws NatsException If the key is invalid or the operation fails
     */
    public function put(string $key, string $value): int
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";
        $ack = $this->js->publish($subject, $value);
        return (int) $ack->sequence;
    }

    /**
     * @throws NatsException If the key is not found or has been deleted
     */
    public function get(string $key): KeyValueEntry
    {
        $this->validateKey($key);

        try {
            $stream = $this->js->stream("KV_{$this->bucketName}");
            $rawMsg = $stream->getLastMessageForSubject("\$KV.{$this->bucketName}.{$key}");
        } catch (JetStreamException $e) {
            if ($e->apiError?->errCode === 10037) { // No message found
                throw new NatsException("Key not found: {$key}");
            }
            throw $e;
        }

        $operation = KeyValueOperation::Put;
        if ($rawMsg->headers !== null) {
            $kvOp = $rawMsg->headers->get('KV-Operation');
            if ($kvOp !== null) {
                $operation = KeyValueOperation::from($kvOp);
            }
        }

        if ($operation !== KeyValueOperation::Put) {
            throw new NatsException("Key not found: {$key}");
        }

        return new KeyValueEntry(
            key: $key,
            value: $rawMsg->data,
            revision: $rawMsg->sequence,
            delta: 0,
            operation: $operation,
            timestamp: $rawMsg->time ?? new \DateTimeImmutable(),
            bucket: $this->bucketName,
        );
    }

    /**
     * @throws NatsException If the key or revision is not found
     */
    public function getRevision(string $key, int $revision): KeyValueEntry
    {
        $this->validateKey($key);

        $stream = $this->js->stream("KV_{$this->bucketName}");

        try {
            $rawMsg = $stream->getMessage($revision);
        } catch (JetStreamException $e) {
            if ($e->apiError?->errCode === 10037) {
                throw new NatsException("Revision not found: {$revision}");
            }
            throw $e;
        }

        // Verify the message subject matches the expected key
        $expectedSubject = "\$KV.{$this->bucketName}.{$key}";
        if ($rawMsg->subject !== $expectedSubject) {
            throw new NatsException("Revision {$revision} does not belong to key: {$key}");
        }

        $operation = KeyValueOperation::Put;
        if ($rawMsg->headers !== null) {
            $kvOp = $rawMsg->headers->get('KV-Operation');
            if ($kvOp !== null) {
                $operation = KeyValueOperation::from($kvOp);
            }
        }

        return new KeyValueEntry(
            key: $key,
            value: $rawMsg->data,
            revision: $rawMsg->sequence,
            delta: 0,
            operation: $operation,
            timestamp: $rawMsg->time ?? new \DateTimeImmutable(),
            bucket: $this->bucketName,
        );
    }

    public function create(string $key, string $value, ?float $keyTtl = null): int
    {
        $this->validateKey($key);

        $headers = new Headers();
        $headers->set('Nats-Expected-Last-Subject-Sequence', '0');

        if ($keyTtl !== null) {
            $headers->set('Nats-TTL', (string) ((int) ($keyTtl * 1_000_000_000)));
        }

        $subject = "\$KV.{$this->bucketName}.{$key}";
        $msg = new Message(subject: $subject, data: $value, headers: $headers);

        try {
            $ack = $this->js->publishMessage($msg);
            return (int) $ack->sequence;
        } catch (JetStreamException $e) {
            if ($e->apiError?->errCode === 10071) { // Wrong last sequence
                throw new NatsException("Key already exists: {$key}");
            }
            throw $e;
        }
    }

    public function update(string $key, string $value, int $revision): int
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";
        $ack = $this->js->publish($subject, $value, PublishOptions::expectLastSequencePerSubject($revision));
        return (int) $ack->sequence;
    }

    public function delete(string $key, ?int $lastRevision = null): void
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";

        $headers = new Headers();
        $headers->set('KV-Operation', KeyValueOperation::Delete->value);

        $opts = [];
        if ($lastRevision !== null) {
            $opts[] = PublishOptions::expectLastSequencePerSubject($lastRevision);
        }

        $msg = new Message(subject: $subject, data: '', headers: $headers);
        $this->js->publishMessage($msg, ...$opts);
    }

    public function purge(string $key, ?float $ttl = null): void
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";

        $headers = new Headers();
        $headers->set('KV-Operation', KeyValueOperation::Purge->value);
        $headers->set('Nats-Rollup', 'sub');

        if ($ttl !== null) {
            $headers->set('Nats-TTL', (string) ((int) ($ttl * 1_000_000_000)));
        }

        $msg = new Message(subject: $subject, data: '', headers: $headers);
        $this->js->publishMessage($msg);
    }

    public function purgeDeletes(?float $ttl = null): void
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $prefix = "\$KV.{$this->bucketName}.";

        $consumer = $stream->orderedConsumer(new OrderedConsumerConfig(
            headersOnly: true,
        ));

        // Track the latest operation and timestamp per key
        $deletedKeys = [];
        $defaultTtl = 30 * 60.0; // 30 minutes, matching nats.go default
        $effectiveTtl = ($ttl === null || $ttl === 0.0) ? $defaultTtl : $ttl;
        $cutoffTs = microtime(true) - abs($effectiveTtl);

        while (true) {
            $batch = $consumer->fetch(1000, new FetchOptions(timeout: 1.0));

            $count = 0;
            foreach ($batch->messages() as $msg) {
                $count++;
                $subject = $msg->subject();
                $key = substr($subject, strlen($prefix));

                $operation = KeyValueOperation::Put;
                $headers = $msg->headers();
                if ($headers !== null) {
                    $kvOp = $headers->get('KV-Operation');
                    if ($kvOp !== null) {
                        $operation = KeyValueOperation::from($kvOp);
                    }
                }

                if ($operation === KeyValueOperation::Put) {
                    unset($deletedKeys[$key]);
                } else {
                    $deletedKeys[$key] = $msg->metadata()->timestamp;
                }

                $msg->ack();
            }

            if ($count === 0) {
                break;
            }
        }

        // Purge subjects whose latest operation is DEL or PURGE.
        // Negative TTL means always remove markers.
        // Markers older than threshold: purge entire subject (including tombstone).
        // Markers newer than threshold: purge with keep=1 (reclaim history, preserve tombstone).
        foreach ($deletedKeys as $key => $timestamp) {
            $keep = null;

            if ($effectiveTtl > 0) {
                $markerTs = (float) $timestamp->format('U.u');
                if ($markerTs > $cutoffTs) {
                    $keep = 1;
                }
            }

            $stream->purge(new StreamPurgeOptions(
                filter: $prefix . $key,
                keep: $keep,
            ));
        }
    }

    public function listKeys(): \Generator
    {
        $prefix = "\$KV.{$this->bucketName}.";
        yield from $this->listLatestKeys(filterSubject: $prefix . '>');
    }

    public function listKeysFiltered(string ...$filters): \Generator
    {
        if ($filters === []) {
            yield from $this->listKeys();
            return;
        }

        $prefix = "\$KV.{$this->bucketName}.";

        $filterSubjects = array_values(array_map(
            fn(string $f) => $prefix . $f,
            $filters,
        ));

        yield from $this->listLatestKeys(filterSubjects: $filterSubjects);
    }

    public function watch(string $keys = '>', WatchOptions ...$opts): KeyWatcher
    {
        return new KeyWatcher($this, $keys, array_values($opts));
    }

    /**
     * @param list<string> $keys
     */
    public function watchFiltered(array $keys, WatchOptions ...$opts): KeyWatcher
    {
        return new KeyWatcher($this, '>', array_values($opts), filterSubjects: $keys);
    }

    public function watchAll(WatchOptions ...$opts): KeyWatcher
    {
        return $this->watch('>', ...$opts);
    }

    /**
     * @return list<KeyValueEntry>
     */
    public function history(string $key): array
    {
        $this->validateKey($key);
        $entries = [];

        $kvSubject = "\$KV.{$this->bucketName}.{$key}";
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $streamInfo = $stream->info();

        for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
            try {
                $rawMsg = $stream->getMessage($seq);
                if ($rawMsg->subject === $kvSubject) {
                    $operation = KeyValueOperation::Put;
                    if ($rawMsg->headers !== null) {
                        $kvOp = $rawMsg->headers->get('KV-Operation');
                        if ($kvOp !== null) {
                            $operation = KeyValueOperation::from($kvOp);
                        }
                    }
                    $entries[] = new KeyValueEntry(
                        key: $key,
                        value: $rawMsg->data,
                        revision: $rawMsg->sequence,
                        delta: 0,
                        operation: $operation,
                        timestamp: $rawMsg->time ?? new \DateTimeImmutable(),
                        bucket: $this->bucketName,
                    );
                }
            } catch (\Throwable) {
                continue;
            }
        }

        return $entries;
    }

    public function status(): KeyValueStatus
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $info = $stream->info();

        $config = new KeyValueConfig(
            bucket: $this->bucketName,
            description: $info->config->description,
            maxBytes: $info->config->maxBytes,
            history: $info->config->maxMessagesPerSubject ?? 1,
            ttl: $info->config->maxAge !== null ? (float) $info->config->maxAge : null,
            maxValueSize: $info->config->maxMsgSize,
            replicas: $info->config->replicas,
            metadata: $info->config->metadata,
            mirror: $info->config->mirror,
            sources: $info->config->sources,
            compression: $info->config->compression !== \Sofascore\Nats\Enum\StoreCompression::None,
            rePublish: $info->config->rePublish,
        );

        return new KeyValueStatus(
            bucket: $this->bucketName,
            values: $info->state->messages,
            history: $info->config->maxMessagesPerSubject ?? 1,
            ttl: $info->config->maxAge,
            bytes: $info->state->bytes,
            backingStore: $info->config->storage->value,
            isCompressed: $info->config->compression !== \Sofascore\Nats\Enum\StoreCompression::None,
            config: $config,
            metadata: $info->config->metadata,
            streamInfo: $info,
        );
    }

    public function bucket(): string
    {
        return $this->bucketName;
    }

    /** @internal */
    public function jetStream(): JetStreamContext
    {
        return $this->js;
    }

    /** @internal */
    public function entryFromJetStreamMsg(JetStreamMessage $msg): KeyValueEntry
    {
        $subject = $msg->subject();
        $key = substr($subject, strlen("\$KV.{$this->bucketName}."));

        $operation = $this->keyOperationFromHeaders($msg->headers());

        $meta = $msg->metadata();

        return new KeyValueEntry(
            key: $key,
            value: $msg->data(),
            revision: $meta->streamSequence,
            delta: $meta->numPending,
            operation: $operation,
            timestamp: $meta->timestamp,
            bucket: $this->bucketName,
        );
    }

    private function validateKey(string $key): void
    {
        if ($key === '' || str_contains($key, ' ') || str_starts_with($key, '.') || str_ends_with($key, '.')) {
            throw new NatsException("Invalid KV key: {$key}");
        }
    }

    /**
     * @param list<string> $filterSubjects
     * @return \Generator<string>
     */
    private function listLatestKeys(?string $filterSubject = null, array $filterSubjects = []): \Generator
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $prefix = "\$KV.{$this->bucketName}.";
        $batchSize = 1000;

        $consumer = $stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::LastPerSubject,
            filterSubject: $filterSubject,
            filterSubjects: $filterSubjects,
            headersOnly: true,
        ));

        while (true) {
            $batch = $consumer->fetch($batchSize, new FetchOptions(timeout: 0.25));
            $count = 0;

            foreach ($batch->messages() as $msg) {
                $count++;

                if ($this->keyOperationFromHeaders($msg->headers()) !== KeyValueOperation::Put) {
                    continue;
                }

                yield substr($msg->subject(), strlen($prefix));
            }

            if ($count < $batchSize) {
                break;
            }
        }
    }

    private function keyOperationFromHeaders(?Headers $headers): KeyValueOperation
    {
        if ($headers === null) {
            return KeyValueOperation::Put;
        }

        $kvOp = $headers->get('KV-Operation');
        if ($kvOp === null) {
            return KeyValueOperation::Put;
        }

        return KeyValueOperation::from($kvOp);
    }

}
