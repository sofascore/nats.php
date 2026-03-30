<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\ObjectStore;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\ObjectStore\ObjectInfo;
use Sofascore\Nats\ObjectStore\ObjectResult;

final class ObjectResultTest extends TestCase
{
    public function testReadConsumesChunksLazily(): void
    {
        $cleanupCalls = 0;
        $result = new ObjectResult(
            $this->createInfo(data: 'hello world', chunks: 3),
            static function (): \Generator {
                yield 'hello';
                yield ' ';
                yield 'world';
            },
            function () use (&$cleanupCalls): void {
                $cleanupCalls++;
            },
        );

        self::assertSame('hell', $result->read(4));
        self::assertSame('o w', $result->read(3));
        self::assertSame('orld', $result->readAll());
        self::assertSame('', $result->read(1));
        self::assertSame(1, $cleanupCalls);
    }

    public function testWriteToStreamsRemainingContent(): void
    {
        $result = new ObjectResult(
            $this->createInfo(data: 'abcdefghij', chunks: 3),
            static function (): \Generator {
                yield 'abc';
                yield 'defg';
                yield 'hij';
            },
        );

        self::assertSame('ab', $result->read(2));

        $stream = fopen('php://temp', 'w+b');
        self::assertNotFalse($stream);

        try {
            self::assertSame(8, $result->writeTo($stream));

            rewind($stream);
            self::assertSame('cdefghij', stream_get_contents($stream));
        } finally {
            fclose($stream);
            $result->close();
        }
    }

    public function testCloseRunsCleanupOnce(): void
    {
        $cleanupCalls = 0;
        $result = new ObjectResult(
            $this->createInfo(data: 'hello', chunks: 1),
            static function (): \Generator {
                yield 'hello';
            },
            function () use (&$cleanupCalls): void {
                $cleanupCalls++;
            },
        );

        $result->close();
        $result->close();

        self::assertSame(1, $cleanupCalls);
        self::assertSame('', $result->readAll());
    }

    public function testReadAllThrowsWhenStreamEndsBeforeExpectedBytes(): void
    {
        $cleanupCalls = 0;
        $result = new ObjectResult(
            $this->createInfo(data: 'hello world', chunks: 3),
            static function (): \Generator {
                yield 'hello';
            },
            function () use (&$cleanupCalls): void {
                $cleanupCalls++;
            },
        );

        try {
            $this->expectException(NatsException::class);
            $this->expectExceptionMessage('Object read incomplete');
            $result->readAll();
        } finally {
            self::assertSame(1, $cleanupCalls);
        }
    }

    public function testDigestMismatchRunsCleanupOnce(): void
    {
        $cleanupCalls = 0;
        $result = new ObjectResult(
            $this->createInfo(data: 'hello', chunks: 1, digest: 'SHA-256=invalid'),
            static function (): \Generator {
                yield 'hello';
            },
            function () use (&$cleanupCalls): void {
                $cleanupCalls++;
            },
        );

        try {
            $this->expectException(NatsException::class);
            $this->expectExceptionMessage('Object digest mismatch');
            $result->readAll();
        } finally {
            self::assertSame(1, $cleanupCalls);
        }
    }

    public function testReadAllThrowsOnDigestMismatch(): void
    {
        $result = new ObjectResult(
            $this->createInfo(data: 'hello', chunks: 1, digest: 'SHA-256=invalid'),
            static function (): \Generator {
                yield 'hello';
            },
        );

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Object digest mismatch');

        $result->readAll();
    }

    private function createInfo(string $data, int $chunks, ?string $digest = null): ObjectInfo
    {
        return new ObjectInfo(
            name: 'file.bin',
            bucket: 'files',
            nuid: 'nuid',
            size: strlen($data),
            chunks: $chunks,
            digest: $digest ?? 'SHA-256=' . base64_encode(hash('sha256', $data, true)),
            deleted: false,
        );
    }
}
