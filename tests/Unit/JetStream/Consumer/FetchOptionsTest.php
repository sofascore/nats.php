<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream\Consumer;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\JetStream\Consumer\FetchOptions;

final class FetchOptionsTest extends TestCase
{
    public function testPositiveTimeoutIsAccepted(): void
    {
        $options = new FetchOptions(timeout: 0.1);

        self::assertSame(0.1, $options->timeout);
    }

    public function testZeroTimeoutIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('timeout must be greater than 0');

        new FetchOptions(timeout: 0.0);
    }

    public function testNegativeTimeoutIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('timeout must be greater than 0');

        new FetchOptions(timeout: -1.0);
    }
}
