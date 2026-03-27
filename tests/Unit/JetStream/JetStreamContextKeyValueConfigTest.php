<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\KeyValue\KeyValueConfig;

final class JetStreamContextKeyValueConfigTest extends TestCase
{
    public function testBuildKvStreamConfigWiresLimitMarkerTtlIntoSubjectDeleteMarkerTtl(): void
    {
        $context = (new \ReflectionClass(JetStreamContext::class))->newInstanceWithoutConstructor();
        $method = new \ReflectionMethod(JetStreamContext::class, 'buildKvStreamConfig');

        $streamConfig = $method->invoke(
            $context,
            new KeyValueConfig(
                bucket: 'cache',
                allowMsgTtl: true,
                limitMarkerTtl: 300.0,
            ),
        );

        self::assertTrue($streamConfig->allowMsgTtl);
        self::assertSame(300, $streamConfig->subjectDeleteMarkerTtl);
    }
}
