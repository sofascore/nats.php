<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Sofascore\Nats\Connection;
use Sofascore\Nats\ConnectionOptions;
use Sofascore\Nats\Enum\ConnectionStatus;
use Sofascore\Nats\Protocol\Parser;
use Sofascore\Nats\Protocol\Writer;
use Sofascore\Nats\ReconnectBufferExceededException;
use Sofascore\Nats\Transport\TransportInterface;

final class ReconnectBufferExceededTest extends TestCase
{
    /**
     * Create a Connection in Reconnecting state with a given buffer size limit.
     */
    private function createReconnectingConnection(int $bufferSize): Connection
    {
        $transport = $this->createMock(TransportInterface::class);

        $options = new ConnectionOptions(reconnectBufferSize: $bufferSize);

        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $ref->getProperty('options')->setValue($conn, $options);
        $ref->getProperty('transport')->setValue($conn, $transport);
        $ref->getProperty('parser')->setValue($conn, new Parser());
        $ref->getProperty('writer')->setValue($conn, new Writer());
        $ref->getProperty('lastError')->setValue($conn, null);
        $ref->getProperty('status')->setValue($conn, ConnectionStatus::Reconnecting);
        $ref->getProperty('reconnectBuffer')->setValue($conn, '');

        return $conn;
    }

    private function callWriteToServer(Connection $conn, string $data): void
    {
        $ref = new \ReflectionClass(Connection::class);
        $method = $ref->getMethod('writeToServer');
        $method->invoke($conn, $data);
    }

    private function getReconnectBuffer(Connection $conn): string
    {
        $ref = new \ReflectionClass(Connection::class);

        return $ref->getProperty('reconnectBuffer')->getValue($conn);
    }

    public function testBufferFillsUpToLimitWithoutError(): void
    {
        $conn = $this->createReconnectingConnection(10);

        // Write exactly 10 bytes — should not throw
        $this->callWriteToServer($conn, 'aaaaaaaaaa');

        self::assertSame('aaaaaaaaaa', $this->getReconnectBuffer($conn));
    }

    public function testBufferExceededThrowsException(): void
    {
        $conn = $this->createReconnectingConnection(10);

        // Fill buffer to limit
        $this->callWriteToServer($conn, 'aaaaaaaaaa');

        // One more byte should throw
        $this->expectException(ReconnectBufferExceededException::class);
        $this->expectExceptionMessage('Reconnect buffer exceeded (10 bytes)');

        $this->callWriteToServer($conn, 'b');
    }

    public function testBufferContentsNotCorruptedByFailedWrite(): void
    {
        $conn = $this->createReconnectingConnection(10);

        // Fill buffer partially
        $this->callWriteToServer($conn, 'hello');

        self::assertSame('hello', $this->getReconnectBuffer($conn));

        // Attempt a write that would exceed the buffer
        try {
            $this->callWriteToServer($conn, '123456');
        } catch (ReconnectBufferExceededException) {
            // expected
        }

        // Buffer must still contain only the original data
        self::assertSame('hello', $this->getReconnectBuffer($conn));
    }
}
