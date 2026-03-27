<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\ObjectStore;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\ObjectStore\ObjectInfo;
use Sofascore\Nats\ObjectStore\ObjectResult;

final class ObjectResultTest extends TestCase
{
    public function testReadConsumesChunksLazily(): void
    {
        $cleanupCalls = 0;
        $result = new ObjectResult(
            $this->createInfo(size: 11),
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
            $this->createInfo(size: 10),
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
            $this->createInfo(size: 5),
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

    private function createInfo(int $size): ObjectInfo
    {
        return new ObjectInfo(
            name: 'file.bin',
            bucket: 'files',
            nuid: 'nuid',
            size: $size,
            chunks: 1,
            digest: 'SHA-256=test',
            deleted: false,
        );
    }
}
