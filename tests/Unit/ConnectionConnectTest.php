<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit;

use Sofascore\Nats\Connection;
use Sofascore\Nats\ConnectionOptions;
use Sofascore\Nats\Enum\ConnectionStatus;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\Protocol\Parser;
use Sofascore\Nats\Protocol\Writer;
use Sofascore\Nats\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

/**
 * Tests for connect-time hostname resolution behavior in connectToServer().
 */
final class ConnectionConnectTest extends TestCase
{
    /**
     * Create a Connection with a mock transport injected via reflection.
     */
    private function createConnectionWithTransport(
        TransportInterface $transport,
        ?ConnectionOptions $options = null,
    ): Connection {
        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $ref->getProperty('options')->setValue($conn, $options ?? new ConnectionOptions());
        $ref->getProperty('transport')->setValue($conn, $transport);
        $ref->getProperty('transportFactory')->setValue($conn, static fn () => new MockTransport(
            failAddresses: $transport->failAddresses(),
            tlsRequired: $transport->tlsRequired(),
        ));
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

        $this->callConnectToServer($conn, 'nats://127.0.0.1:4222');

        self::assertSame(['tcp://127.0.0.1:4222'], $transport->connectAddresses);
    }

    public function testMultiIpFailoverTriesAllAddresses(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://10.0.0.1:4222']);
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(
                customHostResolver: fn (string $host): array => ['10.0.0.1', '10.0.0.2'],
            ),
        );

        $this->callConnectToServer($conn, 'nats://my-server:4222');

        self::assertSame([
            'tcp://10.0.0.1:4222',
            'tcp://10.0.0.2:4222',
        ], $transport->connectAddresses);
    }

    public function testMultiIpFailoverSucceedsOnSecondIp(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://10.0.0.1:4222']);
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(
                customHostResolver: fn (string $host): array => ['10.0.0.1', '10.0.0.2'],
            ),
        );

        // Should not throw — second IP succeeds
        $this->callConnectToServer($conn, 'nats://my-server:4222');

        self::assertCount(2, $transport->connectAddresses);
    }

    public function testAllIpsFailThrowsException(): void
    {
        $transport = new MockTransport(failAddresses: [
            'tcp://10.0.0.1:4222',
            'tcp://10.0.0.2:4222',
        ]);
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(
                customHostResolver: fn (string $host): array => ['10.0.0.1', '10.0.0.2'],
            ),
        );

        $this->expectException(NatsException::class);
        $this->callConnectToServer($conn, 'nats://my-server:4222');
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

    public function testTlsUpgradePeerNameSetToOriginalHostname(): void
    {
        $transport = new MockTransport(tlsRequired: true);
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(skipHostLookup: true),
        );

        $this->callConnectToServer($conn, 'nats://my-nats-server:4222');

        self::assertNotNull($transport->lastUpgradeTlsContext);
        self::assertArrayHasKey('peer_name', $transport->lastUpgradeTlsContext);
        self::assertSame('my-nats-server', $transport->lastUpgradeTlsContext['peer_name']);
    }

    public function testConnectDefaultPort(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport($transport);

        $this->callConnectToServer($conn, 'nats://127.0.0.1');

        self::assertSame(['tcp://127.0.0.1:4222'], $transport->connectAddresses);
    }

    public function testConnectedAddrReturnsNullWithoutLiveStreamResource(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport($transport);

        $ref = new \ReflectionClass(Connection::class);
        $ref->getProperty('serverPool')->setValue($conn, ['nats://configured-host:4222']);
        $ref->getProperty('currentServerIndex')->setValue($conn, 0);
        $ref->getProperty('status')->setValue($conn, ConnectionStatus::Connected);

        self::assertNull($conn->connectedAddr());
    }

    public function testTimeoutDividedAcrossResolvedIps(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://10.0.0.1:4222']);
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(
                timeout: 4.0,
                customHostResolver: fn (string $host): array => ['10.0.0.1', '10.0.0.2'],
            ),
        );

        $this->callConnectToServer($conn, 'nats://my-server:4222');

        // 2 IPs with 4s total → 2s per IP
        self::assertSame([2.0, 2.0], $transport->connectTimeouts);
    }

    public function testSingleIpGetsFullTimeout(): void
    {
        $transport = new MockTransport();
        $conn = $this->createConnectionWithTransport(
            $transport,
            new ConnectionOptions(timeout: 3.0),
        );

        $this->callConnectToServer($conn, 'nats://127.0.0.1:4222');

        self::assertSame([3.0], $transport->connectTimeouts);
    }

    public function testDoConnectThrowsImmediatelyWhenRetryOnFailedConnectIsFalse(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://127.0.0.1:4222']);
        $options = new ConnectionOptions(retryOnFailedConnect: false);
        $conn = $this->createConnectionWithTransport($transport, $options);

        $ref = new \ReflectionClass(Connection::class);
        $ref->getProperty('serverPool')->setValue($conn, ['nats://127.0.0.1:4222']);

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Failed to connect to any NATS server');

        $ref->getMethod('doConnect')->invoke($conn);
    }

    public function testDoConnectReturnsInReconnectingStateWhenRetryOnFailedConnectIsTrue(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://127.0.0.1:4222']);
        $options = new ConnectionOptions(
            retryOnFailedConnect: true,
            maxReconnects: 5,
            reconnectWait: 0.0,
        );
        $conn = $this->createConnectionWithTransport($transport, $options);

        $ref = new \ReflectionClass(Connection::class);
        $ref->getProperty('serverPool')->setValue($conn, ['nats://127.0.0.1:4222']);

        // doConnect() should return without throwing — connection is in Reconnecting state
        $ref->getMethod('doConnect')->invoke($conn);

        self::assertSame(ConnectionStatus::Reconnecting, $conn->status());
        self::assertTrue($conn->isReconnecting());
    }

    public function testReconnectCycleDrivenByProcessWhenRetryOnFailedConnect(): void
    {
        // Start with a transport that fails all connections
        $transport = new MockTransport(failAddresses: ['tcp://127.0.0.1:4222']);
        $options = new ConnectionOptions(
            retryOnFailedConnect: true,
            maxReconnects: 2,
            reconnectWait: 0.0,
        );
        $conn = $this->createConnectionWithTransport($transport, $options);

        $ref = new \ReflectionClass(Connection::class);
        $ref->getProperty('serverPool')->setValue($conn, ['nats://127.0.0.1:4222']);

        // doConnect() returns in Reconnecting state
        $ref->getMethod('doConnect')->invoke($conn);
        self::assertSame(ConnectionStatus::Reconnecting, $conn->status());

        // Now set a transport factory that produces a succeeding transport
        $ref->getProperty('transportFactory')->setValue($conn, static fn () => new MockTransport());

        // Drive a reconnect cycle via processIncoming — transport is not connected so it triggers attemptReconnectCycle
        $ref->getMethod('processIncoming')->invoke($conn, 0.0);

        // Should now be connected
        self::assertSame(ConnectionStatus::Connected, $conn->status());
        self::assertTrue($conn->isConnected());
    }

    public function testReconnectCycleRespectsMaxReconnects(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://127.0.0.1:4222']);
        $options = new ConnectionOptions(
            retryOnFailedConnect: true,
            maxReconnects: 2,
            reconnectWait: 0.0,
        );
        $conn = $this->createConnectionWithTransport($transport, $options);

        $ref = new \ReflectionClass(Connection::class);
        $ref->getProperty('serverPool')->setValue($conn, ['nats://127.0.0.1:4222']);

        // doConnect() returns in Reconnecting state
        $ref->getMethod('doConnect')->invoke($conn);
        self::assertSame(ConnectionStatus::Reconnecting, $conn->status());

        // Drive reconnect cycles — each one fails, incrementing reconnectAttempts
        // With maxReconnects=2, we get 2 failed attempts, then the 3rd call detects max reached
        $ref->getMethod('processIncoming')->invoke($conn, 0.0); // attempt 1
        self::assertSame(ConnectionStatus::Reconnecting, $conn->status());

        $ref->getMethod('processIncoming')->invoke($conn, 0.0); // attempt 2
        self::assertSame(ConnectionStatus::Reconnecting, $conn->status());

        $ref->getMethod('processIncoming')->invoke($conn, 0.0); // detects max reached, closes
        self::assertSame(ConnectionStatus::Closed, $conn->status());
    }

    public function testRetryOnFailedConnectFiresOnConnectOnFirstSuccess(): void
    {
        $transport = new MockTransport(failAddresses: ['tcp://127.0.0.1:4222']);
        $onConnectCalled = false;
        $options = new ConnectionOptions(
            retryOnFailedConnect: true,
            maxReconnects: 5,
            reconnectWait: 0.0,
            onConnect: function () use (&$onConnectCalled): void {
                $onConnectCalled = true;
            },
        );
        $conn = $this->createConnectionWithTransport($transport, $options);

        $ref = new \ReflectionClass(Connection::class);
        $ref->getProperty('serverPool')->setValue($conn, ['nats://127.0.0.1:4222']);

        $ref->getMethod('doConnect')->invoke($conn);
        self::assertFalse($onConnectCalled);

        // Now set a transport factory that produces a succeeding transport
        $ref->getProperty('transportFactory')->setValue($conn, static fn () => new MockTransport());

        $ref->getMethod('processIncoming')->invoke($conn, 0.0);

        self::assertTrue($onConnectCalled, 'onConnect should fire on first successful connect');
        self::assertSame(ConnectionStatus::Connected, $conn->status());
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

    /** @var list<float> All timeouts passed to connect() */
    public array $connectTimeouts = [];

    /** @var array<string, mixed>|null Last TLS context passed to connect() */
    public ?array $lastTlsContext = null;

    /** @var array<string, mixed>|null Last TLS context passed to upgradeTls() */
    public ?array $lastUpgradeTlsContext = null;

    /** @var list<string> Buffered lines for readLine() */
    private array $readBuffer = [];

    private bool $connected = false;

    /**
     * @param list<string> $failAddresses Addresses that should throw on connect
     */
    public function __construct(
        private readonly array $failAddresses = [],
        private readonly bool $tlsRequired = false,
    ) {}

    /** @return list<string> */
    public function failAddresses(): array
    {
        return $this->failAddresses;
    }

    public function tlsRequired(): bool
    {
        return $this->tlsRequired;
    }

    public function connect(string $address, float $timeout, ?array $tlsContext = null): void
    {
        $this->connectAddresses[] = $address;
        $this->connectTimeouts[] = $timeout;
        $this->lastTlsContext = $tlsContext;

        if (in_array($address, $this->failAddresses, true)) {
            throw new NatsException("Mock: connection refused to {$address}");
        }

        $info = json_encode([
            'server_id' => 'MOCK',
            'server_name' => 'mock-server',
            'version' => '2.10.0',
            'proto' => 1,
            'max_payload' => 1_048_576,
            'headers' => true,
            'tls_required' => $this->tlsRequired,
            'connect_urls' => [],
        ], JSON_THROW_ON_ERROR);

        $this->readBuffer = [
            "INFO {$info}\r\n",
            "PONG\r\n",
        ];
        $this->connected = true;
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

    public function close(): void
    {
        $this->connected = false;
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function setBlocking(bool $blocking): void {}

    public function setTimeout(float $seconds): void {}

    public function waitForData(float $timeout): bool
    {
        return $this->connected;
    }

    public function upgradeTls(array $tlsContext): void
    {
        $this->lastUpgradeTlsContext = $tlsContext;
    }

    public function getStreamResource(): mixed
    {
        return null;
    }
}
