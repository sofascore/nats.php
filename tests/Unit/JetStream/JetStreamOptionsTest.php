<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Sofascore\Nats\JetStream\JetStreamOptions;

final class JetStreamOptionsTest extends TestCase
{
    public function testDefaults(): void
    {
        $options = new JetStreamOptions();

        self::assertSame(0, $options->apiRetries);
        self::assertSame(0.25, $options->apiRetryWait);
    }

    public function testCustomValues(): void
    {
        $options = new JetStreamOptions(apiRetries: 3, apiRetryWait: 0.5);

        self::assertSame(3, $options->apiRetries);
        self::assertSame(0.5, $options->apiRetryWait);
    }

    public function testZeroRetriesAllowed(): void
    {
        $options = new JetStreamOptions(apiRetries: 0, apiRetryWait: 0.0);

        self::assertSame(0, $options->apiRetries);
        self::assertSame(0.0, $options->apiRetryWait);
    }

    public function testNegativeRetriesThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('apiRetries must be >= 0');

        new JetStreamOptions(apiRetries: -1);
    }

    public function testNegativeRetryWaitThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('apiRetryWait must be >= 0');

        new JetStreamOptions(apiRetryWait: -0.1);
    }
}
