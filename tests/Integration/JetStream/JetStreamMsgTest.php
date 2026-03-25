<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\JetStream;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\Connection;
use Sofascore\Nats\Message;
use Sofascore\Nats\JetStream\Message\JetStreamMsg;
use Sofascore\Nats\Tests\Integration\NatsUrlTrait;

final class JetStreamMsgTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
    }

    protected function tearDown(): void
    {
        $this->conn->close();
    }

    public function testNakWithDelayPublishesDelayPayload(): void
    {
        $inbox = $this->conn->newInbox();
        $sub = $this->conn->subscribeSync($inbox);

        $msg = new JetStreamMsg(
            new Message(subject: 'test.subject', data: 'payload', replyTo: $inbox),
            $this->conn,
        );

        $msg->nakWithDelay(1.0);

        $ack = $sub->nextMessage(1.0);
        self::assertSame('-NAK {"delay":1000000000}', $ack->data);

        $sub->unsubscribe();
    }

    public function testTermWithReasonPublishesReasonPayload(): void
    {
        $inbox = $this->conn->newInbox();
        $sub = $this->conn->subscribeSync($inbox);

        $msg = new JetStreamMsg(
            new Message(subject: 'test.subject', data: 'payload', replyTo: $inbox),
            $this->conn,
        );

        $msg->termWithReason('invalid payload');

        $ack = $sub->nextMessage(1.0);
        self::assertSame('+TERM invalid payload', $ack->data);

        $sub->unsubscribe();
    }
}
