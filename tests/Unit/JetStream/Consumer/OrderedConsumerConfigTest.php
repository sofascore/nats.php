<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream\Consumer;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\JetStream\Consumer\OrderedConsumerConfig;

final class OrderedConsumerConfigTest extends TestCase
{
    public function testPositiveMaxResetAttemptsIsAccepted(): void
    {
        $config = new OrderedConsumerConfig(maxResetAttempts: 2);

        self::assertSame(2, $config->maxResetAttempts);
    }

    public function testZeroMaxResetAttemptsIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxResetAttempts must be greater than 0');

        new OrderedConsumerConfig(maxResetAttempts: 0);
    }

    public function testNegativeMaxResetAttemptsIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxResetAttempts must be greater than 0');

        new OrderedConsumerConfig(maxResetAttempts: -1);
    }
}
