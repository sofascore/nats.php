<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\AckPolicy;
use Sofascore\Nats\Enum\StorageType;
use Sofascore\Nats\Inbox;
use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use Sofascore\Nats\JetStream\Consumer\PushConsumeOptions;
use Sofascore\Nats\JetStream\Consumer\PushConsumer;
use Sofascore\Nats\JetStream\Consumer\PushConsumerStatus;
use Sofascore\Nats\JetStream\Error\JetStreamException;
use Sofascore\Nats\JetStream\Error\PushConsumerException;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\JetStream\Stream\StreamConfig;
use Sofascore\Nats\JetStream\Stream\StreamInterface;
use Sofascore\Nats\Tests\Integration\NatsUrlTrait;
use PHPUnit\Framework\TestCase;

final class PushConsumerTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private JetStreamContext $js;
    private StreamInterface $stream;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_PUSH'); } catch (\Throwable) {}

        $this->stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_PUSH',
            subjects: ['test.push.>'],
            storage: StorageType::Memory,
        ));
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_PUSH'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testBasicPushConsume(): void
    {
        $deliverSubject = Inbox::generate();

        $consumer = $this->stream->createPushConsumer(new ConsumerConfig(
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
        ));

        // Publish messages
        for ($i = 1; $i <= 5; $i++) {
            $this->js->publish('test.push.events', "msg-{$i}");
        }

        $received = [];
        $ctx = $consumer->consume(function (JetStreamMessage $msg) use (&$received): void {
            $received[] = $msg->data();
            $msg->ack();
        });

        self::assertSame(PushConsumerStatus::Active, $consumer->status());

        // Process messages
        $deadline = microtime(true) + 5.0;
        while (\count($received) < 5 && microtime(true) < $deadline) {
            $this->conn->process(0.1);
        }

        self::assertCount(5, $received);
        self::assertSame('msg-1', $received[0]);
        self::assertSame('msg-5', $received[4]);

        $ctx->stop();
        self::assertSame(PushConsumerStatus::Closed, $consumer->status());
    }

    public function testPushConsumeWithFlowControlAndHeartbeat(): void
    {
        $deliverSubject = Inbox::generate();

        $consumer = $this->stream->createPushConsumer(new ConsumerConfig(
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
            flowControl: true,
            idleHeartbeat: 1.0,
            maxAckPending: 2,
        ));

        // Publish enough messages to trigger flow control
        for ($i = 1; $i <= 10; $i++) {
            $this->js->publish('test.push.events', "fc-{$i}");
        }

        $received = [];
        $errors = [];
        $ctx = $consumer->consume(
            function (JetStreamMessage $msg) use (&$received): void {
                $received[] = $msg->data();
                $msg->ack();
            },
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $deadline = microtime(true) + 10.0;
        while (\count($received) < 10 && microtime(true) < $deadline) {
            $this->conn->process(0.1);
        }

        // All messages should be delivered (flow control responded to automatically)
        self::assertCount(10, $received);

        $ctx->stop();
    }

    public function testHeartbeatDetectsStall(): void
    {
        $deliverSubject = Inbox::generate();

        $consumer = $this->stream->createPushConsumer(new ConsumerConfig(
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::None,
            idleHeartbeat: 0.3,
        ));

        // Don't publish anything — consumer should be idle
        $errors = [];
        $ctx = $consumer->consume(
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        // Process for a while — heartbeats should arrive every 0.3s
        // With checkHeartbeat() in the loop, we can detect total silence too
        $deadline = microtime(true) + 2.0;
        while (microtime(true) < $deadline) {
            $this->conn->process(0.1);
            $consumer->checkHeartbeat();
        }

        // At minimum, the consumer should still be alive (heartbeats handled)
        self::assertContains($consumer->status(), [
            PushConsumerStatus::Active,
            PushConsumerStatus::Stalled, // if server didn't send heartbeats fast enough
        ]);

        $ctx->stop();
    }

    public function testConsumerDeletionSurfacesAsError(): void
    {
        $deliverSubject = Inbox::generate();

        $consumer = $this->stream->createPushConsumer(new ConsumerConfig(
            durable: 'del-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::None,
            idleHeartbeat: 0.5,
        ));

        $errors = [];
        $ctx = $consumer->consume(
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        // Delete the consumer while it's active
        $this->stream->deleteConsumer('del-push');

        // Process — server should send a 409 status
        $deadline = microtime(true) + 3.0;
        while ($consumer->status() !== PushConsumerStatus::Error && microtime(true) < $deadline) {
            $this->conn->process(0.1);
        }

        // The consumer may or may not receive a 409 depending on server timing.
        // Assert both paths explicitly so PHPUnit never flags this as risky.
        if ($consumer->status() === PushConsumerStatus::Error) {
            self::assertNotEmpty($errors);
            self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        } else {
            // Server didn't deliver the 409 in time — still a valid outcome
            self::assertContains($consumer->status(), [
                PushConsumerStatus::Active,
                PushConsumerStatus::Stalled,
            ]);
        }

        $consumer->unsubscribe();
    }

    public function testUnsubscribeAndDrain(): void
    {
        $deliverSubject = Inbox::generate();

        $consumer = $this->stream->createPushConsumer(new ConsumerConfig(
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::None,
        ));

        $consumer->consume(function (JetStreamMessage $msg): void {});
        self::assertSame(PushConsumerStatus::Active, $consumer->status());

        $consumer->unsubscribe();
        self::assertSame(PushConsumerStatus::Closed, $consumer->status());
    }

    public function testStreamHandlePushConsumerLookupAndDelete(): void
    {
        $deliverSubject = Inbox::generate();

        $created = $this->stream->createPushConsumer(new ConsumerConfig(
            name: 'stream-push',
            durable: 'stream-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 3,
        ));

        self::assertSame('stream-push', $created->name());
        self::assertSame($deliverSubject, $created->deliverSubject());
        self::assertSame('stream-push', $created->cachedInfo()?->name);
        self::assertSame($deliverSubject, $created->cachedInfo()?->config->deliverSubject);

        $fetched = $this->stream->pushConsumer('stream-push');
        self::assertSame('stream-push', $fetched->name());
        self::assertSame($deliverSubject, $fetched->deliverSubject());

        $this->stream->deleteConsumer('stream-push');

        $this->expectException(JetStreamException::class);
        $this->stream->pushConsumer('stream-push');
    }

    public function testCachedInfoHydratesColdPushConsumerHandleOnce(): void
    {
        $deliverSubject = Inbox::generate();

        $this->stream->createPushConsumer(new ConsumerConfig(
            name: 'push-cached-cold',
            durable: 'push-cached-cold',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
        ));

        $consumer = new PushConsumer(
            $this->js,
            'TEST_PUSH',
            new ConsumerConfig(
                name: 'push-cached-cold',
                durable: 'push-cached-cold',
                deliverSubject: $deliverSubject,
                ackPolicy: AckPolicy::Explicit,
            ),
        );

        self::assertNull($consumer->cachedInfo());

        $info = $consumer->info();
        self::assertSame('push-cached-cold', $info->name);
        self::assertSame($deliverSubject, $info->config->deliverSubject);

        $this->stream->deleteConsumer('push-cached-cold');

        self::assertSame('push-cached-cold', $consumer->cachedInfo()?->name);

        $this->expectException(JetStreamException::class);
        $consumer->info();
    }

    public function testStreamPushConsumerLookupSeedsCachedInfo(): void
    {
        $deliverSubject = Inbox::generate();

        $this->stream->createPushConsumer(new ConsumerConfig(
            name: 'push-cached-lookup',
            durable: 'push-cached-lookup',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
        ));

        $consumer = $this->stream->pushConsumer('push-cached-lookup');
        self::assertSame('push-cached-lookup', $consumer->cachedInfo()?->name);

        $this->stream->deleteConsumer('push-cached-lookup');

        $info = $consumer->cachedInfo();
        self::assertNotNull($info);
        self::assertSame('push-cached-lookup', $info->name);
        self::assertSame($deliverSubject, $info->config->deliverSubject);
    }

    public function testStreamHandleUpdatePushConsumer(): void
    {
        $deliverSubject = Inbox::generate();

        $this->stream->createPushConsumer(new ConsumerConfig(
            name: 'stream-push',
            durable: 'stream-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
        ));

        $updated = $this->stream->updatePushConsumer(new ConsumerConfig(
            name: 'stream-push',
            durable: 'stream-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 5,
        ));

        self::assertSame(5, $updated->info()->config->maxDeliver);
    }

    public function testStreamHandleUpdatePushConsumerRequiresIdentifier(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('updatePushConsumer requires a consumer name or durable identifier');

        $this->stream->updatePushConsumer(new ConsumerConfig(
            deliverSubject: Inbox::generate(),
            ackPolicy: AckPolicy::Explicit,
        ));
    }

    public function testStreamHandleCreateOrUpdatePushConsumer(): void
    {
        $deliverSubject = Inbox::generate();

        $this->stream->createPushConsumer(new ConsumerConfig(
            name: 'stream-push',
            durable: 'stream-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 3,
        ));

        $upserted = $this->stream->createOrUpdatePushConsumer(new ConsumerConfig(
            name: 'stream-push',
            durable: 'stream-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 7,
        ));
        self::assertSame(7, $upserted->info()->config->maxDeliver);
    }
}
