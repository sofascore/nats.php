<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream\Publish;

use Sofascore\Nats\JetStream\Publish\PubAck;
use Sofascore\Nats\JetStream\Publish\PubAckFuture;
use Sofascore\Nats\Message;
use PHPUnit\Framework\TestCase;

final class PubAckFutureTest extends TestCase
{
    public function testOkThrowsLogicExceptionWhenFutureIsIncomplete(): void
    {
        $future = new PubAckFuture(new Message(subject: 'test.subject'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('PubAckFuture is not complete');

        $future->ok();
    }

    public function testOkReturnsResolvedAck(): void
    {
        $future = new PubAckFuture(new Message(subject: 'test.subject'));
        $future->resolve(new PubAck(stream: 'TEST', sequence: 42));

        $ack = $future->ok();

        self::assertSame('TEST', $ack->stream);
        self::assertSame(42, $ack->sequence);
    }
}
