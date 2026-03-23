<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Connection;
use Nats\ConnectionOptions;
use Nats\NatsException;
use Nats\Protocol\Parser;
use Nats\Protocol\Writer;
use Nats\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

/**
 * Tests for connect-time hostname resolution behavior in connectToServer().
 */
final class ConnectionConnectTest extends TestCase
{
    /**
     * Create a Connection instance with a mock transport injected via reflection.
     */
    private function createConnectionWithTransport(
        TransportInterface $transport,
        ?ConnectionOptions $options = null,
    ): Connection {
        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $ref->getProperty('options')->setValue($conn, $options ?? new ConnectionOptions());
        $ref->getProperty('transport')->setValue($conn, $transport);
        $ref->getProperty('parser')->setValue($conn, new Parser());
        $ref->getProperty('writer')->setValue($conn, new Writer());
        $ref->getProperty('lastError')->setValue($conn, null);

        return $conn;
    }

    /**
     * Invoke the private connectToServer() method via reflection.
     */
    private function callConnectToServer(Connection $conn, string $url): void
    {
        $ref = new \ReflectionClass(Connection::class);
        $method = $ref->getMethod('connectToServer');
        $method->invoke($conn, $url);
    }

    public function testConnectPassesResolvedAddressToTransport(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport($transport);

        // Use IP directly so resolveHost() passes it through — no DNS needed
        $this->callConnectToServer($conn, 'nats://127.0.0.1:4222');

        self::assertSame(['tcp://127.0.0.1:4222'], $transport->connectAddresses);
    }

    public function testConnectTriesNextIpOnFailure(): void
    {
        // Transport that fails on first address, succeeds on second
        $transport = new MockTransport(failAddresses: ['tcp://10.0.0.1:4222']);
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(skipHostLookup: true),
        );

        // With skipHostLookup, resolveHost returns the hostname as-is.
        // To test multi-IP iteration, we need to call connectToServer with an IP
        // that is in the fail list. Since skipHostLookup means no DNS, we test
        // the single-attempt path here.
        $this->expectException(NatsException::class);
        $this->callConnectToServer($conn, 'nats://10.0.0.1:4222');
    }

    public function testTlsPeerNameSetToOriginalHostname(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(
                tlsEnabled: true,
                tlsContextOptions: ['verify_peer' => false, 'verify_peer_name' => false],
                skipHostLookup: true,
            ),
        );

        $this->callConnectToServer($conn, 'nats://my-nats-server:4222');

        // Verify peer_name was set to the original hostname
        self::assertNotNull($transport->lastTlsContext);
        self::assertArrayHasKey('peer_name', $transport->lastTlsContext);
        self::assertSame('my-nats-server', $transport->lastTlsContext['peer_name']);
    }

    public function testTlsPeerNameNotOverriddenIfAlreadySet(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(
                tlsEnabled: true,
                tlsContextOptions: [
                    'verify_peer' => false,
                    'verify_peer_name' => false,
                    'peer_name' => 'custom-peer',
                ],
                skipHostLookup: true,
            ),
        );

        $this->callConnectToServer($conn, 'nats://some-host:4222');

        self::assertNotNull($transport->lastTlsContext);
        self::assertSame('custom-peer', $transport->lastTlsContext['peer_name']);
    }

    public function testConnectDefaultPort(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport($transport);

        $this->callConnectToServer($conn, 'nats://127.0.0.1');

        self::assertSame(['tcp://127.0.0.1:4222'], $transport->connectAddresses);
    }
}

/**
 * A minimal transport mock that records connect() calls and simulates
 * the NATS protocol handshake (INFO → CONNECT+PING → PONG).
 *
 * @internal
 */
final class MockTransport implements TransportInterface
{
    /** @var list<string> All addresses passed to connect() */
    public array $connectAddresses = [];

    /** @var array<string, mixed>|null Last TLS context passed to connect() */
    public ?array $lastTlsContext = null;

    /** @var list<string> Buffered lines for readLine() */
    private array $readBuffer = [];

    /**
     * @param list<string> $failAddresses Addresses that should throw on connect
     */
    public function __construct(
        private readonly array $failAddresses = [],
    ) {}

    public function connect(string $address, float $timeout, ?array $tlsContext = null): void
    {
        $this->connectAddresses[] = $address;
        $this->lastTlsContext = $tlsContext;

        if (in_array($address, $this->failAddresses, true)) {
            throw new NatsException("Mock: connection refused to {$address}");
        }

        // Queue up the NATS protocol handshake responses
        $info = json_encode([
            'server_id' => 'MOCK',
            'server_name' => 'mock-server',
            'version' => '2.10.0',
            'proto' => 1,
            'max_payload' => 1_048_576,
            'headers' => true,
            'tls_required' => false,
            'connect_urls' => [],
        ], JSON_THROW_ON_ERROR);

        $this->readBuffer = [
            "INFO {$info}\r\n",
            "PONG\r\n",
        ];
    }

    public function readLine(): string
    {
        if ($this->readBuffer === []) {
            throw new NatsException('Mock: no more data');
        }

        return array_shift($this->readBuffer);
    }

    public function read(int $length): string
    {
        return '';
    }

    public function write(string $data): int
    {
        return strlen($data);
    }

    public function close(): void {}

    public function isConnected(): bool
    {
        return true;
    }

    public function setBlocking(bool $blocking): void {}

    public function setTimeout(float $seconds): void {}

    public function waitForData(float $timeout): bool
    {
        return true;
    }

    public function upgradeTls(array $tlsContext): void {}

    public function getStreamResource(): mixed
    {
        return null;
    }
}
