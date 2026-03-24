<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use Sofascore\Nats\Tests\Integration\NatsUrlTrait;
use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\AckPolicy;
use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\Enum\StorageType;
use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use Sofascore\Nats\JetStream\Consumer\ConsumeOptions;
use Sofascore\Nats\JetStream\Consumer\FetchOptions;
use Sofascore\Nats\JetStream\Consumer\OrderedConsumerConfig;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Stream\StreamConfig;
use Sofascore\Nats\JetStream\Stream\StreamInterface;
use Sofascore\Nats\TimeoutException;
use PHPUnit\Framework\TestCase;

final class ConsumerTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private JetStreamContext $js;
    private StreamInterface $stream;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_CONSUMER'); } catch (\Throwable) {}

        $this->stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_CONSUMER',
            subjects: ['test.cons.>'],
            storage: StorageType::Memory,
        ));

        // Seed messages
        for ($i = 1; $i <= 20; $i++) {
            $this->js->publish('test.cons.events', json_encode(['id' => $i]));
        }
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_CONSUMER'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testCreateConsumer(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-consumer-1',
            durable: 'test-consumer-1',
            ackPolicy: AckPolicy::Explicit,
            deliverPolicy: DeliveryPolicy::All,
        ));

        $info = $consumer->info();
        self::assertSame('TEST_CONSUMER', $info->stream);
        self::assertSame('test-consumer-1', $info->name);
        self::assertSame(20, $info->numPending);
    }

    public function testFetch(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-fetch',
            durable: 'test-fetch',
            ackPolicy: AckPolicy::Explicit,
        ));

        $batch = $consumer->fetch(5, new FetchOptions(timeout: 3.0));
        $messages = [];
        foreach ($batch as $msg) {
            $messages[] = json_decode($msg->data(), true);
            $msg->ack();
        }

        self::assertCount(5, $messages);
        self::assertSame(1, $messages[0]['id']);
        self::assertSame(5, $messages[4]['id']);
    }

    public function testFetchNoWait(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-nowait',
            durable: 'test-nowait',
            ackPolicy: AckPolicy::Explicit,
        ));

        $batch = $consumer->fetchNoWait(10);
        $count = 0;
        foreach ($batch as $msg) {
            $count++;
            $msg->ack();
        }

        self::assertGreaterThan(0, $count);
        self::assertLessThanOrEqual(10, $count);
    }

    public function testNext(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-next',
            durable: 'test-next',
            ackPolicy: AckPolicy::Explicit,
        ));

        $msg = $consumer->next(3.0);
        $data = json_decode($msg->data(), true);
        self::assertSame(1, $data['id']);
        $msg->ack();
    }

    public function testMessageMetadata(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-meta',
            durable: 'test-meta',
            ackPolicy: AckPolicy::Explicit,
        ));

        $msg = $consumer->next(3.0);
        $meta = $msg->metadata();

        self::assertGreaterThan(0, $meta->streamSequence);
        self::assertGreaterThan(0, $meta->consumerSequence);
        self::assertSame('TEST_CONSUMER', $meta->stream);
        self::assertSame('test-meta', $meta->consumer);

        $msg->ack();
    }

    public function testAckVariants(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-ack',
            durable: 'test-ack',
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 5,
        ));

        // Regular ack
        $msg = $consumer->next(3.0);
        $msg->ack();

        // Nak (will redeliver)
        $msg = $consumer->next(3.0);
        $msg->nak();

        // InProgress then ack
        $msg = $consumer->next(3.0);
        $msg->inProgress();
        $msg->ack();

        // Term (no more redelivery)
        $msg = $consumer->next(3.0);
        $msg->term();

        self::assertTrue(true); // No exceptions = success
    }

    public function testAckPreventsRedelivery(): void
    {
        $this->js->publish('test.cons.ack', 'ack-test');

        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-ack-no-redeliver',
            durable: 'test-ack-no-redeliver',
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 3,
            filterSubject: 'test.cons.ack',
        ));

        // nak causes redelivery
        $msg = $consumer->next(3.0);
        self::assertNotNull($msg);
        $msg->nak();

        $redelivered = $consumer->next(3.0);
        self::assertNotNull($redelivered);
        self::assertSame('ack-test', $redelivered->data());

        // ack stops redelivery
        $redelivered->ack();

        $this->expectException(TimeoutException::class);
        $consumer->next(1.0);
    }

    public function testNakWithDelay(): void
    {
        $this->js->publish('test.cons.nakdelay', 'nak-delay-test');

        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-nak-delay',
            durable: 'test-nak-delay',
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 3,
            filterSubject: 'test.cons.nakdelay',
        ));

        $msg = $consumer->next(3.0);
        self::assertNotNull($msg);

        $msg->nakWithDelay(1.0);

        $redelivered = $consumer->next(5.0);
        self::assertNotNull($redelivered);
        self::assertSame('nak-delay-test', $redelivered->data());

        $redelivered->ack();
    }

    public function testTerm(): void
    {
        $this->js->publish('test.cons.term', 'term-test');

        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-term',
            durable: 'test-term',
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 3,
            filterSubject: 'test.cons.term',
        ));

        $msg = $consumer->next(3.0);
        self::assertNotNull($msg);
        self::assertSame('term-test', $msg->data());

        $msg->term();

        $this->expectException(TimeoutException::class);
        $consumer->next(1.0);
    }

    public function testTermWithReason(): void
    {
        $this->js->publish('test.cons.termreason', 'term-reason-test');

        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-term-reason',
            durable: 'test-term-reason',
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 3,
            filterSubject: 'test.cons.termreason',
        ));

        $msg = $consumer->next(3.0);
        self::assertNotNull($msg);
        self::assertSame('term-reason-test', $msg->data());

        $msg->termWithReason('invalid payload');

        $this->expectException(TimeoutException::class);
        $consumer->next(1.0);
    }

    public function testMessagesIterator(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-iter',
            durable: 'test-iter',
            ackPolicy: AckPolicy::Explicit,
        ));

        $msgs = $consumer->messages(new ConsumeOptions(
            maxMessages: 10,
            expires: 2.0,
        ));

        $count = 0;
        foreach ($msgs as $msg) {
            $msg->ack();
            $count++;
            if ($count >= 5) {
                $msgs->stop();
            }
        }

        self::assertSame(5, $count);
    }

    public function testOrderedConsumer(): void
    {
        $consumer = $this->stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::All,
        ));

        $batch = $consumer->fetch(5, new FetchOptions(timeout: 3.0));
        $ids = [];
        foreach ($batch as $msg) {
            $ids[] = json_decode($msg->data(), true)['id'];
        }

        self::assertCount(5, $ids);
        // Ordered consumer guarantees order
        for ($i = 0; $i < count($ids) - 1; $i++) {
            self::assertLessThan($ids[$i + 1], $ids[$i]);
        }
    }

    public function testFilterSubject(): void
    {
        // Add some messages with different subjects
        $this->js->publish('test.cons.orders', '{"type":"order"}');
        $this->js->publish('test.cons.payments', '{"type":"payment"}');
        $this->js->publish('test.cons.orders', '{"type":"order2"}');

        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-filter',
            durable: 'test-filter',
            ackPolicy: AckPolicy::Explicit,
            filterSubject: 'test.cons.orders',
        ));

        $batch = $consumer->fetch(10, new FetchOptions(timeout: 2.0));
        $subjects = [];
        foreach ($batch as $msg) {
            $subjects[] = $msg->subject();
            $msg->ack();
        }

        foreach ($subjects as $subject) {
            self::assertSame('test.cons.orders', $subject);
        }
    }

    public function testConsumerInfo(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-info',
            durable: 'test-info',
            ackPolicy: AckPolicy::Explicit,
            description: 'Test consumer',
        ));

        $info = $consumer->info();
        self::assertSame('TEST_CONSUMER', $info->stream);
        self::assertSame('test-info', $info->name);
        self::assertSame(20, $info->numPending);
        self::assertSame(0, $info->numAckPending);
    }

    public function testListConsumers(): void
    {
        $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'list-a', durable: 'list-a',
        ));
        $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'list-b', durable: 'list-b',
        ));

        $names = [];
        foreach ($this->stream->consumerNames() as $name) {
            $names[] = $name;
        }

        self::assertContains('list-a', $names);
        self::assertContains('list-b', $names);
    }

    public function testDeleteConsumer(): void
    {
        $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'to-delete', durable: 'to-delete',
        ));

        $this->stream->deleteConsumer('to-delete');

        $this->expectException(\Sofascore\Nats\JetStream\Error\JetStreamException::class);
        $this->stream->consumer('to-delete');
    }
}
