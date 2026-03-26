<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Sofascore\Nats\JetStream\JetStreamContext;

final class JetStreamContextRetryTest extends TestCase
{
    /**
     * @return \Generator<string, array{string, bool}>
     */
    public static function retryableOperationProvider(): \Generator
    {
        // Exact-match retryable operations
        yield 'INFO' => ['INFO', true];
        yield 'STREAM.LIST' => ['STREAM.LIST', true];
        yield 'STREAM.NAMES' => ['STREAM.NAMES', true];

        // Prefix-match retryable operations
        yield 'STREAM.INFO.mystream' => ['STREAM.INFO.mystream', true];
        yield 'CONSUMER.INFO.stream.consumer' => ['CONSUMER.INFO.stream.consumer', true];
        yield 'CONSUMER.LIST.stream' => ['CONSUMER.LIST.stream', true];
        yield 'CONSUMER.NAMES.stream' => ['CONSUMER.NAMES.stream', true];

        // Non-retryable (write) operations
        yield 'STREAM.CREATE.mystream' => ['STREAM.CREATE.mystream', false];
        yield 'STREAM.UPDATE.mystream' => ['STREAM.UPDATE.mystream', false];
        yield 'STREAM.DELETE.mystream' => ['STREAM.DELETE.mystream', false];
        yield 'CONSUMER.CREATE.stream' => ['CONSUMER.CREATE.stream', false];
        yield 'CONSUMER.DELETE.stream.consumer' => ['CONSUMER.DELETE.stream.consumer', false];
        yield 'STREAM.PURGE.mystream' => ['STREAM.PURGE.mystream', false];
    }

    #[DataProvider('retryableOperationProvider')]
    public function testIsRetryable(string $operation, bool $expected): void
    {
        $method = new \ReflectionMethod(JetStreamContext::class, 'isRetryable');

        // Create a minimal instance via reflection (bypass constructor)
        $context = (new \ReflectionClass(JetStreamContext::class))->newInstanceWithoutConstructor();

        self::assertSame($expected, $method->invoke($context, $operation));
    }
}
