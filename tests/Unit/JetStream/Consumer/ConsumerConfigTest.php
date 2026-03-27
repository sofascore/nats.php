<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream\Consumer;

use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use PHPUnit\Framework\TestCase;

final class ConsumerConfigTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $config = new ConsumerConfig();

        self::assertNull($config->sampleFrequency);
        self::assertNull($config->maxRequestExpires);
        self::assertNull($config->maxRequestMaxBytes);
    }

    public function testConstructorWithNewFields(): void
    {
        $config = new ConsumerConfig(
            sampleFrequency: '100',
            maxRequestExpires: 30.0,
            maxRequestMaxBytes: 1048576,
        );

        self::assertSame('100', $config->sampleFrequency);
        self::assertSame(30.0, $config->maxRequestExpires);
        self::assertSame(1048576, $config->maxRequestMaxBytes);
    }

    public function testToArrayWithSampleFrequency(): void
    {
        $config = new ConsumerConfig(sampleFrequency: '50');
        $array = $config->toArray();

        self::assertSame('50', $array['sample_freq']);
    }

    public function testToArrayWithMaxRequestExpiresNsConversion(): void
    {
        $config = new ConsumerConfig(maxRequestExpires: 5.0);
        $array = $config->toArray();

        self::assertSame(5_000_000_000, $array['max_expires']);
    }

    public function testToArrayWithMaxRequestMaxBytes(): void
    {
        $config = new ConsumerConfig(maxRequestMaxBytes: 524288);
        $array = $config->toArray();

        self::assertSame(524288, $array['max_bytes']);
    }

    public function testToArrayOmitsNullNewFields(): void
    {
        $config = new ConsumerConfig();
        $array = $config->toArray();

        self::assertArrayNotHasKey('sample_freq', $array);
        self::assertArrayNotHasKey('max_expires', $array);
        self::assertArrayNotHasKey('max_bytes', $array);
    }

    public function testFromArrayWithNewFields(): void
    {
        $data = [
            'sample_freq' => '75',
            'max_expires' => 10_000_000_000,
            'max_bytes' => 2097152,
        ];

        $config = ConsumerConfig::fromArray($data);

        self::assertSame('75', $config->sampleFrequency);
        self::assertSame(10.0, $config->maxRequestExpires);
        self::assertSame(2097152, $config->maxRequestMaxBytes);
    }

    public function testFromArrayMaxExpiresNsConversion(): void
    {
        $config = ConsumerConfig::fromArray(['max_expires' => 1_500_000_000]);

        self::assertSame(1.5, $config->maxRequestExpires);
    }

    public function testFromArrayDefaults(): void
    {
        $config = ConsumerConfig::fromArray([]);

        self::assertNull($config->sampleFrequency);
        self::assertNull($config->maxRequestExpires);
        self::assertNull($config->maxRequestMaxBytes);
    }

    public function testRoundTripNewFields(): void
    {
        $original = new ConsumerConfig(
            name: 'test-consumer',
            sampleFrequency: '100',
            maxRequestExpires: 15.0,
            maxRequestMaxBytes: 4096,
        );

        $restored = ConsumerConfig::fromArray($original->toArray());

        self::assertSame($original->name, $restored->name);
        self::assertSame($original->sampleFrequency, $restored->sampleFrequency);
        self::assertSame($original->maxRequestExpires, $restored->maxRequestExpires);
        self::assertSame($original->maxRequestMaxBytes, $restored->maxRequestMaxBytes);
    }

    public function testMaxAckPendingOmittedWhenAckNone(): void
    {
        $config = new ConsumerConfig(
            ackPolicy: \Sofascore\Nats\Enum\AckPolicy::None,
        );
        $array = $config->toArray();

        self::assertArrayNotHasKey('max_ack_pending', $array);
    }

    public function testMaxAckPendingIncludedWhenAckExplicit(): void
    {
        $config = new ConsumerConfig(
            ackPolicy: \Sofascore\Nats\Enum\AckPolicy::Explicit,
            maxAckPending: 500,
        );
        $array = $config->toArray();

        self::assertSame(500, $array['max_ack_pending']);
    }

    public function testMaxWaitingOmittedForPushConsumer(): void
    {
        $config = new ConsumerConfig(
            deliverSubject: 'deliver.test',
            maxWaiting: 512,
        );
        $array = $config->toArray();

        self::assertArrayNotHasKey('max_waiting', $array);
    }

    public function testMaxWaitingIncludedForPullConsumer(): void
    {
        $config = new ConsumerConfig(maxWaiting: 256);
        $array = $config->toArray();

        self::assertSame(256, $array['max_waiting']);
    }

    public function testRoundTripMaxRequestMaxBytesPreserved(): void
    {
        $original = new ConsumerConfig(maxRequestMaxBytes: 1048576);

        $array = $original->toArray();
        self::assertSame(1048576, $array['max_bytes']);

        $restored = ConsumerConfig::fromArray($array);
        self::assertSame(1048576, $restored->maxRequestMaxBytes);
    }

    public function testMaxRequestMaxBytesNullOmitsMaxBytes(): void
    {
        $config = new ConsumerConfig(maxRequestMaxBytes: null);
        $array = $config->toArray();

        self::assertArrayNotHasKey('max_bytes', $array);
    }

    public function testToArrayAckWaitNsConversion(): void
    {
        $config = new ConsumerConfig(ackWait: 30.0);
        $array = $config->toArray();

        self::assertSame(30_000_000_000, $array['ack_wait']);
    }

    public function testFromArrayAckWaitNsConversion(): void
    {
        $config = ConsumerConfig::fromArray(['ack_wait' => 5_000_000_000]);

        self::assertSame(5.0, $config->ackWait);
    }

    public function testRoundTripFullConfig(): void
    {
        $original = new ConsumerConfig(
            name: 'full-test',
            maxBatch: 100,
            memoryStorage: true,
            sampleFrequency: '50',
            maxRequestExpires: 60.0,
            maxRequestMaxBytes: 2097152,
        );

        $array = $original->toArray();
        $restored = ConsumerConfig::fromArray($array);

        self::assertSame($original->name, $restored->name);
        self::assertSame($original->maxBatch, $restored->maxBatch);
        self::assertSame($original->memoryStorage, $restored->memoryStorage);
        self::assertSame($original->sampleFrequency, $restored->sampleFrequency);
        self::assertSame($original->maxRequestExpires, $restored->maxRequestExpires);
        self::assertSame($original->maxRequestMaxBytes, $restored->maxRequestMaxBytes);
    }
}
