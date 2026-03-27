<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Unit\JetStream\Consumer;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Headers;
use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use Sofascore\Nats\JetStream\Consumer\PushConsumeContext;
use Sofascore\Nats\JetStream\Consumer\PushConsumeOptions;
use Sofascore\Nats\JetStream\Consumer\PushConsumer;
use Sofascore\Nats\JetStream\Consumer\PushConsumerStatus;
use Sofascore\Nats\JetStream\Error\PushConsumerException;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\JetStream\JetStreamOptions;
use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for PushConsumer protocol handling.
 *
 * Since Connection and JetStreamContext are final classes, we use reflection
 * to construct instances without calling constructors and to invoke private
 * methods directly. This follows the same pattern used in other unit tests
 * in this project (e.g. SubscriptionTypeTest).
 */
final class PushConsumerTest extends TestCase
{
    /** @var list<array{string, string}> Captured publish calls (subject, data) */
    private array $published = [];

    /**
     * Create a PushConsumer wired to a stub Connection that captures publish() calls
     * and a JetStreamContext that delegates connection().
     */
    private function createConsumer(
        ?float $idleHeartbeat = null,
        bool $flowControl = false,
    ): PushConsumer {
        return new PushConsumer(
            $this->createJetStreamContext(),
            'TEST_STREAM',
            new ConsumerConfig(
                name: 'test-push',
                deliverSubject: 'deliver.test',
                flowControl: $flowControl,
                idleHeartbeat: $idleHeartbeat,
            ),
        );
    }

    private function createJetStreamContext(): JetStreamContext
    {
        $conn = $this->createStubConnection();

        $ref = new \ReflectionClass(JetStreamContext::class);
        $js = $ref->newInstanceWithoutConstructor();

        $connProp = $ref->getProperty('conn');
        $connProp->setValue($js, $conn);

        $optsProp = $ref->getProperty('options');
        $optsProp->setValue($js, new JetStreamOptions());

        return $js;
    }

    private function createStubConnection(): Connection
    {
        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        return $conn;
    }

    /**
     * Activate a PushConsumer by calling consume(), capturing the subscription handler.
     * Since we can't actually subscribe via the stub Connection, we use reflection
     * to set internal state and capture the wrapped handler.
     *
     * @return \Closure The wrapped message handler that can be called to simulate message delivery
     */
    private function activateConsumer(
        PushConsumer $consumer,
        \Closure $userHandler,
        ?PushConsumeOptions $opts = null,
    ): \Closure {
        $opts ??= new PushConsumeOptions();
        $ref = new \ReflectionClass(PushConsumer::class);

        // Set up internal state as consume() would
        $ref->getProperty('consumeOptions')->setValue($consumer, $opts);
        $ref->getProperty('userHandler')->setValue($consumer, $userHandler);
        $ref->getProperty('heartbeatIntervalSeconds')->setValue(
            $consumer,
            (new \ReflectionClass(PushConsumer::class))->getProperty('heartbeatIntervalSeconds')->getValue($consumer) ?: 0.0,
        );

        // Read config to get idleHeartbeat
        $config = $ref->getProperty('config')->getValue($consumer);
        $ref->getProperty('heartbeatIntervalSeconds')->setValue($consumer, $config->idleHeartbeat ?? 0.0);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true));
        $ref->getProperty('status')->setValue($consumer, PushConsumerStatus::Active);

        $ctx = new PushConsumeContext($consumer);
        $ref->getProperty('consumeContext')->setValue($consumer, $ctx);

        // Return a closure that invokes handleIncomingMessage directly
        $handleMethod = $ref->getMethod('handleIncomingMessage');

        return function (Message $msg) use ($handleMethod, $consumer): void {
            $handleMethod->invoke($consumer, $msg);
        };
    }

    public function testInitialStatusIsUnsubscribed(): void
    {
        $consumer = $this->createConsumer();
        self::assertSame(PushConsumerStatus::Unsubscribed, $consumer->status());
    }

    public function testDeliversNormalMessages(): void
    {
        $consumer = $this->createConsumer();

        $received = [];
        $deliver = $this->activateConsumer($consumer, function (JetStreamMessage $msg) use (&$received): void {
            $received[] = $msg->data();
        });

        $deliver(new Message(
            subject: '$JS.ACK.TEST_STREAM.test-push.1.1.1.1729000000.0',
            data: 'hello',
        ));

        self::assertCount(1, $received);
        self::assertSame('hello', $received[0]);
    }

    public function testDeliversMessagesWithNonStatusHeaders(): void
    {
        $consumer = $this->createConsumer();

        $received = [];
        $deliver = $this->activateConsumer($consumer, function (JetStreamMessage $msg) use (&$received): void {
            $received[] = $msg->data();
        });

        $headers = new Headers();
        $headers->set('X-Custom', 'value');
        $deliver(new Message(
            subject: '$JS.ACK.TEST_STREAM.test-push.1.1.1.1729000000.0',
            data: 'with-headers',
            headers: $headers,
        ));

        self::assertCount(1, $received);
        self::assertSame('with-headers', $received[0]);
    }

    public function testFlowControlMessageNotDeliveredToUser(): void
    {
        $consumer = $this->createConsumer(flowControl: true, idleHeartbeat: 5.0);

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {
                self::fail('Flow control status messages should not reach user handler');
            },
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $headers = new Headers();
        $headers->set('Status', '100');

        // Flow control with reply subject — publish fails on stub connection (not connected),
        // but the error is caught internally and reported via the error handler.
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            replyTo: '$JS.FC.TEST_STREAM.test-push',
            headers: $headers,
        ));

        // Publish failure reported as error, but status stays Active (not terminal)
        self::assertSame(PushConsumerStatus::Active, $consumer->status());
        // The publish error was caught and reported
        self::assertCount(1, $errors);
        self::assertInstanceOf(NatsException::class, $errors[0]);
    }

    public function testIdleHeartbeatDoesNotDeliverToUser(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 5.0);

        $deliver = $this->activateConsumer($consumer, function (JetStreamMessage $msg): void {
            self::fail('Heartbeat should not reach user handler');
        });

        $headers = new Headers();
        $headers->set('Status', '100');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        self::assertSame(PushConsumerStatus::Active, $consumer->status());
    }

    public function testHeartbeatTimeoutTransitionsToStalled(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 0.001);

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        // Force heartbeat expiry by setting lastHeartbeatTime in the past
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 1.0);

        // Deliver a normal message — heartbeat check runs first
        $deliver(new Message(
            subject: '$JS.ACK.TEST_STREAM.test-push.1.1.1.1729000000.0',
            data: 'late',
        ));

        self::assertCount(1, $errors);
        self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        self::assertSame(100, $errors[0]->statusCode);
        self::assertStringContainsString('No heartbeat received', $errors[0]->getMessage());
    }

    public function testRecoveryFromStalledOnRealMessage(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 0.001);

        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e): void {},
            ),
        );

        // Force stall
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 1.0);

        // This triggers stall detection, then delivers the real message which recovers
        $deliver(new Message(
            subject: '$JS.ACK.TEST_STREAM.test-push.1.1.1.1729000000.0',
            data: 'recovery',
        ));

        // deliverToUser resets status from Stalled → Active
        self::assertSame(PushConsumerStatus::Active, $consumer->status());
    }

    public function testDescriptionHeaderPreferredOverStatusSuffix(): void
    {
        $consumer = $this->createConsumer();

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        // Status header has code only, Description header has the semantic value
        $headers = new Headers();
        $headers->set('Status', '409');
        $headers->set('Description', 'Consumer Deleted');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        // Should be terminal because Description says "Consumer Deleted"
        self::assertSame(PushConsumerStatus::Error, $consumer->status());
        self::assertCount(1, $errors);
        self::assertStringContainsString('Consumer Deleted', $errors[0]->getMessage());
    }

    public function testTerminalStatus409(): void
    {
        $consumer = $this->createConsumer();

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {
                self::fail('Terminal status should not reach user handler');
            },
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $headers = new Headers();
        $headers->set('Status', '409 Consumer Deleted');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        self::assertSame(PushConsumerStatus::Error, $consumer->status());
        self::assertCount(1, $errors);
        self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        self::assertSame(409, $errors[0]->statusCode);
        self::assertStringContainsString('Consumer Deleted', $errors[0]->getMessage());
    }

    public function testStatus409LeadershipChangeIsNotTerminal(): void
    {
        $consumer = $this->createConsumer();

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $headers = new Headers();
        $headers->set('Status', '409 Leadership Changed');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        // Leadership change is transient — status should remain Active
        self::assertSame(PushConsumerStatus::Active, $consumer->status());
        self::assertCount(1, $errors);
        self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        self::assertSame(409, $errors[0]->statusCode);
    }

    public function testStatus409ExceededMaxAckPendingIsNotTerminal(): void
    {
        $consumer = $this->createConsumer();

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $headers = new Headers();
        $headers->set('Status', '409 Exceeded MaxAckPending');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        self::assertSame(PushConsumerStatus::Active, $consumer->status());
        self::assertCount(1, $errors);
    }

    public function testTerminalStatus503(): void
    {
        $consumer = $this->createConsumer();

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $headers = new Headers();
        $headers->set('Status', '503 No Responders');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        self::assertSame(PushConsumerStatus::Error, $consumer->status());
        self::assertCount(1, $errors);
        self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        self::assertSame(503, $errors[0]->statusCode);
    }

    public function testUnsubscribeTransitionsToClosed(): void
    {
        $consumer = $this->createConsumer();

        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('status')->setValue($consumer, PushConsumerStatus::Active);

        $consumer->unsubscribe();

        self::assertSame(PushConsumerStatus::Closed, $consumer->status());
    }

    public function testDrainTransitionsToClosed(): void
    {
        $consumer = $this->createConsumer();

        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('status')->setValue($consumer, PushConsumerStatus::Active);

        $consumer->drain();

        self::assertSame(PushConsumerStatus::Closed, $consumer->status());
    }

    public function testRequiresDeliverSubject(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('deliver_subject');

        new PushConsumer(
            $this->createJetStreamContext(),
            'TEST_STREAM',
            new ConsumerConfig(name: 'no-deliver'),
        );
    }

    public function testErrorHandlerNotRequiredForTerminalStatus(): void
    {
        $consumer = $this->createConsumer();

        // No error handler
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
        );

        $headers = new Headers();
        $headers->set('Status', '409 Consumer Deleted');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        // Should not throw — error is stored on context
        self::assertSame(PushConsumerStatus::Error, $consumer->status());
    }

    public function testHeartbeatNotCheckedWhenNotConfigured(): void
    {
        // No idleHeartbeat configured
        $consumer = $this->createConsumer(idleHeartbeat: null);

        $received = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg) use (&$received): void {
                $received[] = $msg->data();
            },
        );

        // Force lastHeartbeatTime way in the past — should not trigger stall
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 999.0);

        $deliver(new Message(
            subject: '$JS.ACK.TEST_STREAM.test-push.1.1.1.1729000000.0',
            data: 'ok',
        ));

        self::assertSame(PushConsumerStatus::Active, $consumer->status());
        self::assertCount(1, $received);
    }

    public function testUnknownStatusReportsError(): void
    {
        $consumer = $this->createConsumer();

        $errors = [];
        $deliver = $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {
                self::fail('Unknown status should not reach user handler');
            },
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $headers = new Headers();
        $headers->set('Status', '299 Custom');
        $deliver(new Message(
            subject: 'deliver.test',
            data: '',
            headers: $headers,
        ));

        // Unknown non-error status: reported but not terminal
        self::assertCount(1, $errors);
        self::assertInstanceOf(PushConsumerException::class, $errors[0]);
        self::assertSame(299, $errors[0]->statusCode);
        // Status should NOT be Error (only >=400 is terminal)
        self::assertSame(PushConsumerStatus::Active, $consumer->status());
    }

    public function testCheckHeartbeatDetectsStalledWithoutMessage(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 0.001);

        $errors = [];
        $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        // Force heartbeat expiry
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 1.0);

        // Call checkHeartbeat() directly — no message delivery needed
        $consumer->checkHeartbeat();

        self::assertSame(PushConsumerStatus::Stalled, $consumer->status());
        self::assertCount(1, $errors);
        self::assertInstanceOf(PushConsumerException::class, $errors[0]);
    }

    public function testCheckHeartbeatNoOpWhenClosed(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 0.001);

        $errors = [];
        $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        $consumer->unsubscribe();
        self::assertSame(PushConsumerStatus::Closed, $consumer->status());

        // Force expired heartbeat and call checkHeartbeat — should no-op
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 999.0);

        $consumer->checkHeartbeat();

        // Status must remain Closed, not regress to Stalled
        self::assertSame(PushConsumerStatus::Closed, $consumer->status());
        self::assertCount(0, $errors);
    }

    public function testCheckHeartbeatNoOpWhenError(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 0.001);

        $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
        );

        // Force Error state
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('status')->setValue($consumer, PushConsumerStatus::Error);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 999.0);

        $consumer->checkHeartbeat();

        self::assertSame(PushConsumerStatus::Error, $consumer->status());
    }

    public function testHeartbeatPausedDuringReconnect(): void
    {
        $consumer = $this->createConsumer(idleHeartbeat: 0.001);

        $errors = [];
        $this->activateConsumer(
            $consumer,
            function (JetStreamMessage $msg): void {},
            new PushConsumeOptions(
                errorHandler: function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
        );

        // Force expired heartbeat
        $ref = new \ReflectionClass(PushConsumer::class);
        $ref->getProperty('lastHeartbeatTime')->setValue($consumer, microtime(true) - 999.0);

        // Set connection status to Reconnecting
        $js = $ref->getProperty('js')->getValue($consumer);
        $conn = $js->connection();
        $connRef = new \ReflectionClass(\Sofascore\Nats\Connection::class);
        $connRef->getProperty('status')->setValue($conn, \Sofascore\Nats\Enum\ConnectionStatus::Reconnecting);

        // checkHeartbeat should no-op during reconnect
        $consumer->checkHeartbeat();

        // Status should remain Active, not Stalled
        self::assertSame(PushConsumerStatus::Active, $consumer->status());
        self::assertCount(0, $errors);
    }

    public function testConsumeRollsBackStateOnSubscribeFailure(): void
    {
        // Stub connection is not connected, so subscribe() will throw
        $consumer = $this->createConsumer();

        try {
            $consumer->consume(function (JetStreamMessage $msg): void {});
            self::fail('Expected exception from subscribe on disconnected connection');
        } catch (NatsException) {
            // Expected
        }

        // Status should remain Unsubscribed, not stuck in Active
        self::assertSame(PushConsumerStatus::Unsubscribed, $consumer->status());

        // Verify internal state was cleaned up
        $ref = new \ReflectionClass(PushConsumer::class);
        self::assertNull($ref->getProperty('consumeOptions')->getValue($consumer));
        self::assertNull($ref->getProperty('userHandler')->getValue($consumer));
    }
}
