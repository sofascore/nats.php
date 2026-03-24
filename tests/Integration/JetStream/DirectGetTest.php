<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use Sofascore\Nats\Tests\Integration\NatsUrlTrait;
use Sofascore\Nats\Connection;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Stream\StreamConfig;
use PHPUnit\Framework\TestCase;

final class DirectGetTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_DIRECT'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_DIRECT'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testDirectGetLastBySubject(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_DIRECT',
            subjects: ['direct.>'],
            allowDirect: true,
        ));

        $this->js->publish('direct.foo', 'first');
        $this->js->publish('direct.foo', 'second');
        $this->js->publish('direct.bar', 'other');

        $msg = $stream->directGet('direct.foo');
        self::assertSame('second', $msg->data);
        self::assertSame('direct.foo', $msg->subject);
        self::assertGreaterThan(0, $msg->sequence);
    }

    public function testDirectGetBySequence(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_DIRECT',
            subjects: ['direct.>'],
            allowDirect: true,
        ));

        $ack1 = $this->js->publish('direct.seq', 'msg-at-seq-1');
        $ack2 = $this->js->publish('direct.seq', 'msg-at-seq-2');

        $msg = $stream->directGet('direct.seq', $ack1->sequence);
        self::assertSame('msg-at-seq-1', $msg->data);

        $msg = $stream->directGet('direct.seq', $ack2->sequence);
        self::assertSame('msg-at-seq-2', $msg->data);
    }

    public function testDirectGetNotFoundThrows(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_DIRECT',
            subjects: ['direct.>'],
            allowDirect: true,
        ));

        $this->expectException(\Throwable::class);
        $stream->directGet('direct.nonexistent');
    }
}
