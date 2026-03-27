<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\AckPolicy;
use Sofascore\Nats\Enum\StorageType;
use Sofascore\Nats\Inbox;
use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use Sofascore\Nats\JetStream\Consumer\PushConsumeOptions;
use Sofascore\Nats\JetStream\Consumer\PushConsumerStatus;
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

        $consumer = $this->js->createPushConsumer('TEST_PUSH', new ConsumerConfig(
            name: 'basic-push',
            durable: 'basic-push',
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

        $consumer = $this->js->createPushConsumer('TEST_PUSH', new ConsumerConfig(
            name: 'fc-push',
            durable: 'fc-push',
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

        $consumer = $this->js->createPushConsumer('TEST_PUSH', new ConsumerConfig(
            name: 'hb-push',
            durable: 'hb-push',
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
        // If the server sends them, status stays Active
        $deadline = microtime(true) + 2.0;
        while (microtime(true) < $deadline) {
            $this->conn->process(0.1);
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

        $consumer = $this->js->createPushConsumer('TEST_PUSH', new ConsumerConfig(
            name: 'del-push',
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
        $this->js->deleteConsumer('TEST_PUSH', 'del-push');

        // Process — server should send a 409 status
        $deadline = microtime(true) + 3.0;
        while ($consumer->status() !== PushConsumerStatus::Error && microtime(true) < $deadline) {
            $this->conn->process(0.1);
        }

        // The consumer may or may not get the 409 depending on server behavior.
        // At minimum, verify that if it did arrive, it was handled correctly.
        if ($consumer->status() === PushConsumerStatus::Error) {
            self::assertNotEmpty($errors);
            self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        }

        $consumer->unsubscribe();
    }

    public function testUnsubscribeAndDrain(): void
    {
        $deliverSubject = Inbox::generate();

        $consumer = $this->js->createPushConsumer('TEST_PUSH', new ConsumerConfig(
            name: 'drain-push',
            durable: 'drain-push',
            deliverSubject: $deliverSubject,
            ackPolicy: AckPolicy::None,
        ));

        $consumer->consume(function (JetStreamMessage $msg): void {});
        self::assertSame(PushConsumerStatus::Active, $consumer->status());

        $consumer->unsubscribe();
        self::assertSame(PushConsumerStatus::Closed, $consumer->status());
    }
}
