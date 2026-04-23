<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Sofascore\Nats\JetStream\JetStreamContext;

final class JetStreamContextStreamNameValidationTest extends TestCase
{
    private static function invokeValidate(string $name): void
    {
        $method = new \ReflectionMethod(JetStreamContext::class, 'validateStreamName');
        $context = (new \ReflectionClass(JetStreamContext::class))->newInstanceWithoutConstructor();
        $method->invoke($context, $name);
    }

    #[DataProvider('validNames')]
    public function testAcceptsValidStreamName(string $name): void
    {
        self::invokeValidate($name);
        $this->addToAssertionCount(1);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function validNames(): iterable
    {
        yield 'simple' => ['orders'];
        yield 'with-dash' => ['my-stream'];
        yield 'with-underscore' => ['KV_cache'];
        yield 'alphanumeric' => ['events123'];
        yield 'mixed-case' => ['MyStream'];
        yield 'object-store-prefix' => ['OBJ_media'];
    }

    #[DataProvider('invalidNames')]
    public function testRejectsInvalidStreamName(string $name): void
    {
        $this->expectException(\InvalidArgumentException::class);
        self::invokeValidate($name);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidNames(): iterable
    {
        yield 'empty' => [''];
        yield 'with-space' => ['my stream'];
        yield 'with-tab' => ["my\tstream"];
        yield 'with-cr' => ["my\rstream"];
        yield 'with-lf' => ["my\nstream"];
        yield 'with-dot' => ['my.stream'];
        yield 'with-star' => ['my*stream'];
        yield 'with-gt' => ['my>stream'];
        yield 'with-slash' => ['my/stream'];
        yield 'with-backslash' => ['my\\stream'];
        yield 'with-null-byte' => ["my\x00stream"];
        yield 'with-del' => ["my\x7fstream"];
        yield 'only-space' => [' '];
    }

    public function testInvalidNameProducesDescriptiveMessage(): void
    {
        try {
            self::invokeValidate('bad.name');
            self::fail('Expected InvalidArgumentException');
        } catch (\InvalidArgumentException $e) {
            self::assertSame(
                "Invalid stream name 'bad.name': cannot contain whitespace, '.', '*', '>', '/', '\\', or control characters",
                $e->getMessage(),
            );
            self::assertSame(0, $e->getCode());
            self::assertNull($e->getPrevious());
        }
    }

    public function testEmptyNameProducesDescriptiveMessage(): void
    {
        try {
            self::invokeValidate('');
            self::fail('Expected InvalidArgumentException');
        } catch (\InvalidArgumentException $e) {
            self::assertSame('Stream name cannot be empty', $e->getMessage());
            self::assertSame(0, $e->getCode());
            self::assertNull($e->getPrevious());
        }
    }
}
