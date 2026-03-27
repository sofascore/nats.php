<?php

declare(strict_types=1);

namespace Sofascore\Nats\ObjectStore;

use Sofascore\Nats\Enum\AckPolicy;
use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\Enum\ReplayPolicy;
use Sofascore\Nats\Enum\StoreCompression;
use Sofascore\Nats\Headers;
use Sofascore\Nats\Inbox;
use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use Sofascore\Nats\JetStream\Consumer\FetchOptions;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Stream\StreamConfig;
use Sofascore\Nats\Internal\TypeCast as T;
use Sofascore\Nats\KeyValue\WatchOptions;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;

final class ObjectStore implements ObjectStoreInterface
{
    private const INTERNAL_CONSUMER_INACTIVE_THRESHOLD = 30.0;
    private const OBJECT_CHUNK_FETCH_BATCH = 32;
    private const META_FETCH_BATCH = 256;

    private int $chunkSize;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $bucketName,
        int $chunkSize = 131072, // 128KB
    ) {
        $this->chunkSize = self::requirePositiveChunkSize($chunkSize);
    }

    /**
     * @throws NatsException If the operation fails
     */
    public function put(ObjectMeta $meta, mixed $data): ObjectInfo
    {
        $content = is_resource($data) ? stream_get_contents($data) : (string) $data;
        $chunkSize = $meta->chunkSize ?? $this->chunkSize;
        return $this->putRaw($meta->name, $content, $meta->description, $meta->headers, $meta->link, $meta->metadata, $chunkSize);
    }

    public function putBytes(string $name, string $data): ObjectInfo
    {
        return $this->putRaw($name, $data);
    }

    public function putString(string $name, string $data): ObjectInfo
    {
        return $this->putRaw($name, $data);
    }

    /**
     * @throws NatsException If the file is not readable or the operation fails
     */
    public function putFile(string $filePath): ObjectInfo
    {
        if (!is_readable($filePath)) {
            throw new NatsException("File not readable: {$filePath}");
        }

        $handle = fopen($filePath, 'rb');
        if ($handle === false) {
            throw new NatsException("Failed to open file: {$filePath}");
        }

        try {
            return $this->putStream(basename($filePath), $handle);
        } finally {
            fclose($handle);
        }
    }

    /**
     * Upload data from a stream resource, reading in chunks to avoid loading the entire content into memory.
     *
     * @param string $name Object name
     * @param mixed $stream Readable stream resource
     * @throws NatsException If the stream is unreadable, the chunk size is invalid, or the upload fails
     */
    public function putStream(string $name, mixed $stream): ObjectInfo
    {
        // TODO: Refactor the shared upload flow in putStream() and putRaw() into a private helper to reduce drift.
        self::assertReadableStream($stream, $name);
        $chunkSize = self::requirePositiveChunkSize($this->chunkSize);
        $jsStream = $this->js->stream("OBJ_{$this->bucketName}");

        // Purge old chunks if overwriting
        try {
            $existing = $this->getObjectInfo($name);
            if (!$existing->deleted && $existing->nuid !== '') {
                $jsStream->purge(new \Sofascore\Nats\JetStream\Stream\StreamPurgeOptions(
                    filter: "\$O.{$this->bucketName}.C.{$existing->nuid}",
                ));
            }
        } catch (\Throwable) {
            // Object doesn't exist yet
        }

        $nuid = Inbox::nuid();
        $chunkSubject = "\$O.{$this->bucketName}.C.{$nuid}";
        $hashCtx = hash_init('sha256');
        $totalSize = 0;
        $chunks = 0;

        try {
            while (!feof($stream)) {
                $chunk = fread($stream, $chunkSize);
                if ($chunk === false) {
                    throw new NatsException("Failed to read from stream for object: {$name}");
                }
                if ($chunk === '') {
                    if (feof($stream)) {
                        break;
                    }

                    throw new NatsException("Failed to read from stream for object: {$name}");
                }

                hash_update($hashCtx, $chunk);
                $this->js->publish($chunkSubject, $chunk);
                $totalSize += strlen($chunk);
                $chunks++;
            }

            if ($chunks === 0) {
                $this->js->publish($chunkSubject, '');
                $chunks = 1;
            }

            $digest = 'SHA-256=' . base64_encode(hash_final($hashCtx, true));

            $info = new ObjectInfo(
                name: $name,
                bucket: $this->bucketName,
                nuid: $nuid,
                size: $totalSize,
                chunks: $chunks,
                digest: $digest,
                deleted: false,
                mtime: new \DateTimeImmutable(),
            );

            $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
            $metaHeaders = new Headers();
            $metaHeaders->set('Nats-Rollup', 'sub');

            $msg = new Message(
                subject: $metaSubject,
                data: json_encode($info->toArray(), JSON_THROW_ON_ERROR),
                headers: $metaHeaders,
            );
            $this->js->publishMessage($msg);
        } catch (\Throwable $e) {
            try {
                $jsStream->purge(new \Sofascore\Nats\JetStream\Stream\StreamPurgeOptions(
                    filter: $chunkSubject,
                ));
            } catch (\Throwable) {
                // Best effort cleanup for partial uploads.
            }

            throw $e;
        }

        return $info;
    }

    /**
     * @throws NatsException If the object is not found, deleted, or is a cross-bucket link
     */
    public function get(string $name): ObjectResult
    {
        $info = $this->resolveReadableObjectInfo($name);
        $consumerName = $this->createInternalConsumerName('OBJR');
        $consumer = $this->js->createConsumer(
            $this->streamName(),
            new ConsumerConfig(
                name: $consumerName,
                deliverPolicy: DeliveryPolicy::All,
                ackPolicy: AckPolicy::None,
                filterSubject: $this->chunkSubject($info->nuid),
                inactiveThreshold: self::INTERNAL_CONSUMER_INACTIVE_THRESHOLD,
                replayPolicy: ReplayPolicy::Instant,
                memoryStorage: true,
            ),
        );

        return new ObjectResult(
            $info,
            function () use ($info, $consumer): \Generator {
                $remaining = max(1, $info->chunks);

                while ($remaining > 0) {
                    $batchSize = min(self::OBJECT_CHUNK_FETCH_BATCH, $remaining);
                    $batch = $consumer->fetch($batchSize, new FetchOptions(timeout: 1.0));
                    $fetched = 0;

                    foreach ($batch->messages() as $message) {
                        yield $message->data();
                        $fetched++;
                        $remaining--;
                    }

                    if ($fetched === 0) {
                        return;
                    }
                }
            },
            function () use ($consumerName): void {
                $this->deleteInternalConsumer($consumerName);
            },
        );
    }

    public function getBytes(string $name): string
    {
        $result = $this->get($name);

        try {
            return $result->readAll();
        } finally {
            $result->close();
        }
    }

    public function getString(string $name): string
    {
        return $this->getBytes($name);
    }

    public function getFile(string $name, string $filePath): void
    {
        $handle = fopen($filePath, 'wb');
        if ($handle === false) {
            throw new NatsException("Failed to write to file: {$filePath}");
        }

        $result = $this->get($name);

        try {
            $written = $result->writeTo($handle);
            if ($written !== $result->info()->size) {
                throw new NatsException("Failed to write to file: {$filePath}");
            }
        } finally {
            $result->close();
            fclose($handle);
        }
    }

    public function getInfo(string $name, bool $showDeleted = false): ObjectInfo
    {
        $info = $this->getObjectInfo($name);
        if (!$showDeleted && $info->deleted) {
            throw new NatsException("Object not found: {$name}");
        }
        return $info;
    }

    /**
     * @throws NatsException If the object is not found or the operation fails
     */
    public function delete(string $name): void
    {
        $info = $this->getObjectInfo($name);

        // Purge chunks
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $stream->purge(new \Sofascore\Nats\JetStream\Stream\StreamPurgeOptions(
            filter: "\$O.{$this->bucketName}.C.{$info->nuid}",
        ));

        // Write delete marker to meta
        $deletedInfo = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: $info->nuid,
            size: 0,
            chunks: 0,
            digest: '',
            deleted: true,
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($deletedInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);
    }

    public function updateMeta(string $name, ObjectMeta $meta): ObjectInfo
    {
        $existing = $this->getObjectInfo($name);

        $updatedInfo = new ObjectInfo(
            name: $meta->name,
            bucket: $this->bucketName,
            nuid: $existing->nuid,
            size: $existing->size,
            chunks: $existing->chunks,
            digest: $existing->digest,
            deleted: false,
            mtime: new \DateTimeImmutable(),
            description: $meta->description,
            headers: $meta->headers,
            link: $meta->link ?? $existing->link,
            metadata: $meta->metadata !== [] ? $meta->metadata : $existing->metadata,
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($meta->name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($updatedInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);

        return $updatedInfo;
    }

    public function addLink(string $name, ObjectInfo $target): ObjectInfo
    {
        if ($target->deleted) {
            throw new NatsException("Cannot link to a deleted object");
        }

        if ($target->isLink()) {
            throw new NatsException("Cannot link to another link");
        }

        $linkInfo = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: '',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            mtime: new \DateTimeImmutable(),
            link: new ObjectLink(
                bucket: $target->bucket,
                name: $target->name,
            ),
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($linkInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);

        return $linkInfo;
    }

    public function addBucketLink(string $name, ObjectStoreInterface $targetStore): ObjectInfo
    {
        $linkInfo = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: '',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            mtime: new \DateTimeImmutable(),
            link: new ObjectLink(
                bucket: $targetStore->bucket(),
            ),
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($linkInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);

        return $linkInfo;
    }

    /**
     * List objects in the store.
     *
     * @param bool $showDeleted Include deleted objects (inverse of Go's IgnoreDeletes)
     * @param bool $includeHistory Include all versions, not just latest (matches Go's IncludeHistory)
     * @return list<ObjectInfo>
     */
    public function list(bool $showDeleted = false, bool $includeHistory = false): array
    {
        $consumerName = $this->createInternalConsumerName($includeHistory ? 'OBJH' : 'OBJL');
        $objects = [];

        try {
            $consumer = $this->js->createConsumer(
                $this->streamName(),
                new ConsumerConfig(
                    name: $consumerName,
                    deliverPolicy: $includeHistory ? DeliveryPolicy::All : DeliveryPolicy::LastPerSubject,
                    ackPolicy: AckPolicy::None,
                    filterSubject: $this->metaSubjectPrefix() . '>',
                    inactiveThreshold: self::INTERNAL_CONSUMER_INACTIVE_THRESHOLD,
                    replayPolicy: ReplayPolicy::Instant,
                    memoryStorage: true,
                ),
            );

            while (true) {
                $batch = $consumer->fetch(self::META_FETCH_BATCH, new FetchOptions(timeout: 1.0));
                $fetched = 0;

                foreach ($batch->messages() as $message) {
                    $info = $this->decodeObjectInfo($message->data());
                    if ($showDeleted || !$info->deleted) {
                        $objects[] = $info;
                    }
                    $fetched++;
                }

                if ($fetched < self::META_FETCH_BATCH) {
                    return $objects;
                }
            }
        } finally {
            $this->deleteInternalConsumer($consumerName);
        }
    }

    public function watch(WatchOptions ...$opts): ObjectWatcher
    {
        return new ObjectWatcher($this->js, $this->bucketName, array_values($opts));
    }

    public function seal(): void
    {
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $currentInfo = $stream->info();

        // Create a new stream config with sealed=true
        $sealedConfig = new StreamConfig(
            name: "OBJ_{$this->bucketName}",
            subjects: $currentInfo->config->subjects,
            description: $currentInfo->config->description,
            retention: $currentInfo->config->retention,
            maxAge: $currentInfo->config->maxAge,
            maxBytes: $currentInfo->config->maxBytes,
            maxMsgSize: $currentInfo->config->maxMsgSize,
            maxMessages: $currentInfo->config->maxMessages,
            maxMessagesPerSubject: $currentInfo->config->maxMessagesPerSubject,
            storage: $currentInfo->config->storage,
            discard: $currentInfo->config->discard,
            replicas: $currentInfo->config->replicas,
            noAck: $currentInfo->config->noAck,
            denyDelete: $currentInfo->config->denyDelete,
            denyPurge: $currentInfo->config->denyPurge,
            allowRollup: $currentInfo->config->allowRollup,
            compression: $currentInfo->config->compression,
            placement: $currentInfo->config->placement,
            metadata: $currentInfo->config->metadata,
            sealed: true,
            allowDirect: $currentInfo->config->allowDirect,
            discardNewPerSubject: $currentInfo->config->discardNewPerSubject,
        );

        $this->js->updateStream($sealedConfig);
    }

    public function status(): ObjectStoreStatus
    {
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $info = $stream->info();

        return new ObjectStoreStatus(
            bucket: $this->bucketName,
            size: $info->state->bytes,
            objects: $info->state->messages,
            config: new ObjectStoreConfig(
                bucket: $this->bucketName,
                description: $info->config->description,
                maxBytes: $info->config->maxBytes,
                storage: $info->config->storage,
                replicas: $info->config->replicas,
                metadata: $info->config->metadata,
                compression: $info->config->compression !== StoreCompression::None,
            ),
            backingStore: $info->config->storage->value,
            metadata: $info->config->metadata,
            sealed: $info->config->sealed,
            isCompressed: $info->config->compression !== StoreCompression::None,
            streamInfo: $info,
        );
    }

    public function bucket(): string
    {
        return $this->bucketName;
    }

    // --- Private ---

    /**
     * @param array<string, string> $metadata
     */
    private function putRaw(
        string $name,
        string $data,
        ?string $description = null,
        ?Headers $headers = null,
        ?ObjectLink $link = null,
        array $metadata = [],
        ?int $chunkSize = null,
    ): ObjectInfo {
        $chunkSize = self::requirePositiveChunkSize($chunkSize ?? $this->chunkSize);

        // Purge old chunks if overwriting
        try {
            $existing = $this->getObjectInfo($name);
            if (!$existing->deleted && $existing->nuid !== '') {
                $stream = $this->js->stream("OBJ_{$this->bucketName}");
                $stream->purge(new \Sofascore\Nats\JetStream\Stream\StreamPurgeOptions(
                    filter: "\$O.{$this->bucketName}.C.{$existing->nuid}",
                ));
            }
        } catch (\Throwable) {
            // Object doesn't exist yet - that's fine
        }

        $nuid = Inbox::nuid();
        $totalSize = strlen($data);
        $chunks = 0;

        // Compute digest
        $digest = 'SHA-256=' . base64_encode(hash('sha256', $data, true));

        // Publish chunks
        $chunkSubject = "\$O.{$this->bucketName}.C.{$nuid}";
        $offset = 0;
        while ($offset < $totalSize) {
            $chunk = substr($data, $offset, $chunkSize);
            $this->js->publish($chunkSubject, $chunk);
            $offset += strlen($chunk);
            $chunks++;
        }

        // Handle empty data
        if ($totalSize === 0) {
            $this->js->publish($chunkSubject, '');
            $chunks = 1;
        }

        // Build object info
        $info = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: $nuid,
            size: $totalSize,
            chunks: $chunks,
            digest: $digest,
            deleted: false,
            mtime: new \DateTimeImmutable(),
            description: $description,
            headers: $headers,
            link: $link,
            metadata: $metadata,
        );

        // Publish metadata with rollup
        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $metaHeaders = new Headers();
        $metaHeaders->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($info->toArray(), JSON_THROW_ON_ERROR),
            headers: $metaHeaders,
        );
        $this->js->publishMessage($msg);

        return $info;
    }

    private function getObjectInfo(string $name): ObjectInfo
    {
        $metaSubject = $this->metaSubject($name);
        $stream = $this->js->stream($this->streamName());

        try {
            $rawMsg = $stream->getLastMessageForSubject($metaSubject);
        } catch (\Throwable) {
            throw new NatsException("Object not found: {$name}");
        }

        return $this->decodeObjectInfo($rawMsg->data);
    }

    private function encodeName(string $name): string
    {
        // Replace special chars that aren't valid in NATS subjects
        return str_replace(['/', ' '], ['_', '_'], $name);
    }

    private function resolveReadableObjectInfo(string $name): ObjectInfo
    {
        $info = $this->getObjectInfo($name);
        if ($info->deleted) {
            throw new NatsException("Object deleted: {$name}");
        }

        if ($info->isLink()) {
            if ($info->link === null || $info->link->name === '') {
                throw new NatsException("Object is a bucket link and cannot be read directly: {$name}");
            }

            if ($info->link->bucket !== '' && $info->link->bucket !== $this->bucketName) {
                throw new NatsException("Object is a link to another bucket: {$info->link->bucket}/{$info->link->name}");
            }

            return $this->resolveReadableObjectInfo($info->link->name);
        }

        if ($info->nuid === '') {
            throw new NatsException("Object metadata is missing chunk information: {$name}");
        }

        return $info;
    }

    private function decodeObjectInfo(string $data): ObjectInfo
    {
        return ObjectInfo::fromArray(T::stringKeyArray(json_decode($data, true, 512, JSON_THROW_ON_ERROR)));
    }

    private function createInternalConsumerName(string $prefix): string
    {
        return $prefix . '_' . Inbox::nuid();
    }

    private function deleteInternalConsumer(string $consumerName): void
    {
        try {
            $this->js->deleteConsumer($this->streamName(), $consumerName);
        } catch (\Throwable) {
            // Best effort cleanup for internal ephemeral readers.
        }
    }

    private function streamName(): string
    {
        return "OBJ_{$this->bucketName}";
    }

    private function chunkSubject(string $nuid): string
    {
        return "\$O.{$this->bucketName}.C.{$nuid}";
    }

    private function metaSubject(string $name): string
    {
        return $this->metaSubjectPrefix() . $this->encodeName($name);
    }

    private function metaSubjectPrefix(): string
    {
        return "\$O.{$this->bucketName}.M.";
    }

    /**
     * @return int<1, max>
     */
    private static function requirePositiveChunkSize(int $chunkSize): int
    {
        if ($chunkSize <= 0) {
            throw new NatsException('Chunk size must be greater than 0');
        }

        return $chunkSize;
    }

    /**
     * @phpstan-assert resource $stream
     */
    private static function assertReadableStream(mixed $stream, string $name): void
    {
        if (!is_resource($stream) || get_resource_type($stream) !== 'stream') {
            throw new NatsException("Stream for object {$name} must be a readable stream resource");
        }

        $mode = stream_get_meta_data($stream)['mode'];
        if (!str_contains($mode, 'r') && !str_contains($mode, '+')) {
            throw new NatsException("Stream for object {$name} must be a readable stream resource");
        }
    }
}
