<?php

declare(strict_types=1);

namespace Sofascore\Nats\ObjectStore;

use Sofascore\Nats\NatsException;

final class ObjectResult
{
    private string $buffer = '';
    private bool $closed = false;
    private bool $sourceStarted = false;
    private bool $sourceExhausted = false;
    private int $bytesRead = 0;
    private int $chunksRead = 0;
    private ?\HashContext $hashContext;
    /** @var null|\Generator<int, string> */
    private ?\Generator $source = null;

    public function __construct(
        private readonly ObjectInfo $info,
        private readonly \Closure $sourceFactory,
        private ?\Closure $onClose = null,
    ) {
        $this->hashContext = hash_init('sha256');
    }

    public function read(int $length): string
    {
        if ($this->closed || $length <= 0) {
            return '';
        }

        $this->fillBuffer($length);

        if ($this->buffer === '') {
            return '';
        }

        $chunk = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);
        return $chunk;
    }

    public function readAll(): string
    {
        $data = $this->buffer;
        $this->buffer = '';

        while (($chunk = $this->nextChunk()) !== null) {
            $data .= $chunk;
        }

        return $data;
    }

    public function info(): ObjectInfo
    {
        return $this->info;
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->buffer = '';
        $this->source = null;
        $this->runCleanup();
    }

    /**
     * Write contents to a stream resource.
     *
     * @param resource $resource
     */
    public function writeTo(mixed $resource): int
    {
        $written = 0;

        while (($chunk = $this->read(8192)) !== '') {
            $offset = 0;
            $length = strlen($chunk);

            while ($offset < $length) {
                $chunkWritten = fwrite($resource, substr($chunk, $offset));
                if ($chunkWritten === false || $chunkWritten === 0) {
                    return $written;
                }

                $written += $chunkWritten;
                $offset += $chunkWritten;
            }
        }

        return $written;
    }

    public function __destruct()
    {
        $this->close();
    }

    private function fillBuffer(int $targetLength): void
    {
        while (!$this->closed && strlen($this->buffer) < $targetLength) {
            $chunk = $this->nextChunk();
            if ($chunk === null) {
                return;
            }

            $this->buffer .= $chunk;
        }
    }

    private function nextChunk(): ?string
    {
        if ($this->closed || $this->sourceExhausted) {
            return null;
        }

        if (!$this->sourceStarted) {
            /** @var \Generator<int, string> $source */
            $source = ($this->sourceFactory)();
            $this->source = $source;
            $this->sourceStarted = true;
        }

        if ($this->source === null || !$this->source->valid()) {
            $this->finishSource();
            return null;
        }

        try {
            $chunk = $this->source->current();
            $this->source->next();
            $this->bytesRead += strlen($chunk);
            $this->chunksRead++;
            $hashContext = $this->hashContext;
            if ($hashContext === null) {
                throw new NatsException("Object digest state unavailable: {$this->info->name}");
            }

            hash_update($hashContext, $chunk);
            return $chunk;
        } catch (\Throwable $error) {
            $this->close();
            throw $error;
        }
    }

    private function finishSource(): void
    {
        if ($this->sourceExhausted) {
            return;
        }

        $error = null;
        try {
            $this->assertCompleteRead();
        } catch (\Throwable $e) {
            $error = $e;
        }

        $this->sourceExhausted = true;
        $this->source = null;
        $this->runCleanup();

        if ($error !== null) {
            throw $error;
        }
    }

    private function assertCompleteRead(): void
    {
        $expectedChunks = max(1, $this->info->chunks);
        if ($this->bytesRead !== $this->info->size || $this->chunksRead !== $expectedChunks) {
            throw new NatsException(
                "Object read incomplete: {$this->info->name} ({$this->bytesRead}/{$this->info->size} bytes)"
            );
        }

        if ($this->info->digest === '') {
            return;
        }

        if ($this->hashContext === null) {
            throw new NatsException("Object digest state unavailable: {$this->info->name}");
        }

        $actualDigest = 'SHA-256=' . base64_encode(hash_final($this->hashContext, true));
        $this->hashContext = null;
        if ($actualDigest !== $this->info->digest) {
            throw new NatsException("Object digest mismatch: {$this->info->name}");
        }
    }

    private function runCleanup(): void
    {
        if ($this->onClose === null) {
            return;
        }

        $cleanup = $this->onClose;
        $this->onClose = null;
        $cleanup();
    }
}
