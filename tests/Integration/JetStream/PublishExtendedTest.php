<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use Sofascore\Nats\Tests\Integration\NatsUrlTrait;
use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\StorageType;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\Publish\PublishOptions;
use Sofascore\Nats\JetStream\Stream\StreamConfig;
use PHPUnit\Framework\TestCase;

final class PublishExtendedTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private JetStreamContext $js;
    private string $streamName;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
        $this->js = $this->conn->jetStream();
        $this->streamName = 'TEST_PEXT_' . strtoupper(substr(uniqid(), -6));

        try { $this->js->deleteStream($this->streamName); } catch (\Throwable) {}

        $prefix = strtolower($this->streamName);
        $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
            duplicateWindow: 120,
        ));
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream($this->streamName); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testExpectLastMsgId(): void
    {
        $prefix = strtolower($this->streamName);

        // Publish first message with msgId
        $ack1 = $this->js->publish(
            "{$prefix}.dedup",
            'first',
            PublishOptions::msgId('msg-alpha'),
        );
        self::assertFalse($ack1->duplicate);

        // Publish second message expecting last msgId to be 'msg-alpha'
        $ack2 = $this->js->publish(
            "{$prefix}.dedup",
            'second',
            PublishOptions::msgId('msg-beta'),
            PublishOptions::expectLastMsgId('msg-alpha'),
        );
        self::assertFalse($ack2->duplicate);
        self::assertSame($ack1->sequence + 1, $ack2->sequence);
    }

    public function testExpectLastMsgIdWrongFails(): void
    {
        $prefix = strtolower($this->streamName);

        $this->js->publish(
            "{$prefix}.dedup2",
            'first',
            PublishOptions::msgId('msg-one'),
        );

        // Expecting wrong last msgId should fail
        $this->expectException(\Sofascore\Nats\JetStream\Error\JetStreamException::class);
        $this->js->publish(
            "{$prefix}.dedup2",
            'second',
            PublishOptions::msgId('msg-two'),
            PublishOptions::expectLastMsgId('wrong-id'),
        );
    }

    public function testStallWait(): void
    {
        $prefix = strtolower($this->streamName);

        // stallWait is advisory; just verify it does not cause an error
        $ack = $this->js->publish(
            "{$prefix}.stall",
            'data',
            PublishOptions::stallWait(5.0),
        );

        self::assertSame($this->streamName, $ack->stream);
        self::assertGreaterThanOrEqual(1, $ack->sequence);
    }
}
