<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream\Consumer;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\Headers;
use Sofascore\Nats\JetStream\Consumer\MessageBatch;
use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\JetStream\Message\MessageMetadata;

final class MessageBatchTest extends TestCase
{
    public function testAllReturnsMessagesInInsertionOrder(): void
    {
        $batch = new MessageBatch();
        $first = $this->message('first');
        $second = $this->message('second');

        $batch->addMessage($first);
        $batch->addMessage($second);

        self::assertSame([$first, $second], $batch->all());
    }

    private function message(string $data): JetStreamMessage
    {
        return new class($data) implements JetStreamMessage {
            public function __construct(private readonly string $data) {}

            public function subject(): string
            {
                return 'test.subject';
            }

            public function data(): string
            {
                return $this->data;
            }

            public function headers(): ?Headers
            {
                return null;
            }

            public function metadata(): MessageMetadata
            {
                throw new \BadMethodCallException('Not needed in this test');
            }

            public function ack(): void {}

            public function ackSync(float $timeout = 5.0): void {}

            public function nak(): void {}

            public function nakWithDelay(float $seconds): void {}

            public function term(): void {}

            public function termWithReason(string $reason): void {}

            public function inProgress(): void {}

            public function doubleAcked(): bool
            {
                return false;
            }
        };
    }
}
