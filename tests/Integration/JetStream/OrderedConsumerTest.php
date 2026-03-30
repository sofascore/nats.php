<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\Enum\StorageType;
use Sofascore\Nats\JetStream\Consumer\OrderedConsumerConfig;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Stream\StreamConfig;
use Sofascore\Nats\JetStream\Stream\StreamInterface;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\Tests\Integration\NatsUrlTrait;

final class OrderedConsumerTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private JetStreamContext $js;
    private StreamInterface $stream;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_ORDERED_CONSUMER'); } catch (\Throwable) {}

        $this->stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_ORDERED_CONSUMER',
            subjects: ['test.ordered.>'],
            storage: StorageType::Memory,
        ));

        for ($i = 1; $i <= 6; $i++) {
            $this->js->publish('test.ordered.events', json_encode(['id' => $i], JSON_THROW_ON_ERROR));
        }
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_ORDERED_CONSUMER'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testOrderedConsumerCarriesForwardCursorAcrossRecreation(): void
    {
        $consumer = $this->stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::All,
        ));

        $first = $consumer->next(1.0);
        $second = $consumer->next(1.0);
        $initialName = $consumer->cachedInfo()->name;

        self::assertSame(1, $first->metadata()->streamSequence);
        self::assertSame(2, $second->metadata()->streamSequence);

        $this->stream->deleteConsumer($initialName);

        $third = $consumer->next(1.0);

        self::assertSame(3, $third->metadata()->streamSequence);
        self::assertSame(1, $third->metadata()->consumerSequence);
        self::assertNotSame($initialName, $consumer->cachedInfo()->name);
    }

    public function testOrderedConsumerRecreatesWhenSequenceTrackingDetectsLoss(): void
    {
        $consumer = $this->stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::All,
        ));

        $first = $consumer->next(1.0);
        $initialName = $consumer->cachedInfo()->name;

        self::assertSame(1, $first->metadata()->streamSequence);

        $property = new \ReflectionProperty($consumer, 'consumerSequence');
        $property->setValue($consumer, 99);

        $second = $consumer->next(1.0);

        self::assertSame(2, $second->metadata()->streamSequence);
        self::assertSame(1, $second->metadata()->consumerSequence);
        self::assertNotSame($initialName, $consumer->cachedInfo()->name);
    }

    public function testOrderedConsumerHonorsConfiguredResetAttemptLimit(): void
    {
        $consumer = $this->stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::All,
            maxResetAttempts: 2,
        ));

        $first = $consumer->next(1.0);
        self::assertSame(1, $first->metadata()->streamSequence);

        $this->js->deleteStream('TEST_ORDERED_CONSUMER');

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Failed to recreate ordered consumer after 2 attempt(s)');

        $consumer->next(0.3);
    }

    public function testOrderedConsumerUsesConfiguredInactiveThreshold(): void
    {
        $consumer = $this->stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::All,
            inactiveThreshold: 45.0,
        ));

        $info = $consumer->info();

        self::assertSame(45.0, $info->config->inactiveThreshold);
    }
}
