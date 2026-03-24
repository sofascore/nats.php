<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Connection;
use Nats\ConnectionOptions;
use PHPUnit\Framework\TestCase;

/**
 * Tests for Connection::resolveHost() — connect-time hostname resolution.
 */
final class ConnectionResolveHostTest extends TestCase
{
    /**
     * Invoke the private resolveHost() method via reflection.
     *
     * @return list<string>
     */
    private function resolveHost(string $host, bool $skipHostLookup = false): array
    {
        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $optionsProp = $ref->getProperty('options');
        $optionsProp->setValue($conn, new ConnectionOptions(skipHostLookup: $skipHostLookup));

        $method = $ref->getMethod('resolveHost');
        $result = $method->invoke($conn, $host);
        \assert(\is_array($result));

        /** @var list<string> $result */
        return $result;
    }

    public function testIpv4AddressPassedThrough(): void
    {
        $result = $this->resolveHost('127.0.0.1');

        self::assertSame(['127.0.0.1'], $result);
    }

    public function testIpv6AddressPassedThrough(): void
    {
        $result = $this->resolveHost('::1');

        self::assertSame(['::1'], $result);
    }

    public function testSkipHostLookupReturnsHostnameDirectly(): void
    {
        $result = $this->resolveHost('localhost', skipHostLookup: true);

        self::assertSame(['localhost'], $result);
    }

    public function testLocalhostResolvesToIp(): void
    {
        $result = $this->resolveHost('localhost');

        self::assertNotEmpty($result);
        // localhost should resolve to at least 127.0.0.1
        self::assertContains('127.0.0.1', $result);
    }

    public function testUnresolvableHostnameFallsBackToOriginal(): void
    {
        // .invalid is a reserved TLD that will never resolve (RFC 2606)
        $hostname = 'this-host-does-not-exist.invalid';
        $result = $this->resolveHost($hostname);

        self::assertSame([$hostname], $result);
    }

    public function testResolvedResultIsNonEmpty(): void
    {
        // Even for an unresolvable host, we always get at least the original back
        $result = $this->resolveHost('unlikely-host-name-999.test');

        self::assertNotEmpty($result);
    }
}
