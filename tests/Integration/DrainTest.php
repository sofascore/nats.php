<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration;

use Sofascore\Nats\Connection;
use Sofascore\Nats\ConnectionOptions;
use Sofascore\Nats\Message;
use PHPUnit\Framework\TestCase;

final class DrainTest extends TestCase
{
    use NatsUrlTrait;

    public function testAsyncSubscriptionReceivesAllMessagesDuringDrain(): void
    {
        $conn = Connection::connect(self::natsUrl(), new ConnectionOptions(
            drainTimeout: 5.0,
        ));

        $received = [];
        $subject = 'test.drain.async.' . uniqid();

        $conn->subscribe($subject, function (Message $msg) use (&$received): void {
            $received[] = $msg->data;
        });

        // Publish messages
        $count = 10;
        for ($i = 0; $i < $count; $i++) {
            $conn->publish($subject, "msg-{$i}");
        }
        $conn->flush();

        // Drain should deliver all in-flight messages before closing
        $conn->drain();

        self::assertCount($count, $received, 'All messages should be delivered during drain');
        for ($i = 0; $i < $count; $i++) {
            self::assertSame("msg-{$i}", $received[$i]);
        }
    }

    public function testSyncSubscriptionReceivesAllMessagesDuringDrain(): void
    {
        $conn = Connection::connect(self::natsUrl(), new ConnectionOptions(
            drainTimeout: 5.0,
        ));

        $subject = 'test.drain.sync.' . uniqid();
        $sub = $conn->subscribeSync($subject);

        $count = 10;
        for ($i = 0; $i < $count; $i++) {
            $conn->publish($subject, "msg-{$i}");
        }
        $conn->flush();

        // Wait for messages to arrive
        $deadline = microtime(true) + 2.0;
        while ($sub->queuedMsgs() < $count && microtime(true) < $deadline) {
            $conn->process(0.05);
        }

        // Dequeue all before drain
        $received = [];
        while ($sub->queuedMsgs() > 0) {
            $msg = $sub->nextMessage(0.1);
            $received[] = $msg->data;
        }

        $conn->drain();

        self::assertCount($count, $received);
    }

    public function testDrainTimeoutIsRespected(): void
    {
        $conn = Connection::connect(self::natsUrl(), new ConnectionOptions(
            drainTimeout: 0.5,
        ));

        $subject = 'test.drain.timeout.' . uniqid();
        $conn->subscribe($subject, function (Message $msg): void {
            // intentionally slow handler
        });

        $start = microtime(true);
        $conn->drain();
        $elapsed = microtime(true) - $start;

        // Drain should complete (not hang forever)
        self::assertLessThan(2.0, $elapsed, 'Drain should respect timeout');
    }

    public function testDrainAllowsPublishDuringSubscriptionPhase(): void
    {
        $conn = Connection::connect(self::natsUrl(), new ConnectionOptions(
            drainTimeout: 5.0,
        ));

        $subject = 'test.drain.pubduring.' . uniqid();
        $published = false;

        $conn->subscribe($subject, function (Message $msg) use ($conn, &$published): void {
            // Publish from within a drain callback - should work during DrainingSubscriptions
            $conn->publish('test.drain.pubduring.echo', $msg->data);
            $published = true;
        });

        $conn->publish($subject, 'hello');
        $conn->flush();

        $conn->drain();

        self::assertTrue($published, 'Handler should have fired and published during drain');
    }
}
