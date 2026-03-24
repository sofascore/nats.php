<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\ObjectStore;

use Sofascore\Nats\ObjectStore\ObjectStoreConfig;
use PHPUnit\Framework\TestCase;

final class ObjectStoreConfigTest extends TestCase
{
    public function testConstructorRejectsNonPositiveMaxChunkSize(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxChunkSize must be greater than 0');

        new ObjectStoreConfig(bucket: 'invalid', maxChunkSize: 0);
    }
}
