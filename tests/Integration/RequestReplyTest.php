<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration;

use Sofascore\Nats\Connection;
use Sofascore\Nats\ConnectionOptions;
use Sofascore\Nats\Headers;
use Sofascore\Nats\Message;
use Sofascore\Nats\TimeoutException;
use PHPUnit\Framework\TestCase;

final class RequestReplyTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private string $connectionName;

    protected function setUp(): void
    {
        $this->connectionName = 'request-reply-test-' . uniqid();
        $this->conn = Connection::connect(self::natsUrl(), new ConnectionOptions(name: $this->connectionName));
    }

    protected function tearDown(): void
    {
        $this->conn->close();
    }

    public function testRequestReply(): void
    {
        $this->conn->subscribe('test.rr.echo', function (Message $msg): void {
            $msg->respond('echo: ' . $msg->data);
        });

        $reply = $this->conn->request('test.rr.echo', 'hello', 2.0);
        self::assertSame('echo: hello', $reply->data);
    }

    public function testRequestReplyJson(): void
    {
        $this->conn->subscribe('test.rr.add', function (Message $msg): void {
            $data = json_decode($msg->data, true);
            $result = ($data['a'] ?? 0) + ($data['b'] ?? 0);
            $msg->respond(json_encode(['result' => $result]));
        });

        $reply = $this->conn->request('test.rr.add', json_encode(['a' => 5, 'b' => 3]), 2.0);
        $result = json_decode($reply->data, true);
        self::assertSame(8, $result['result']);
    }

    public function testRequestReplyWithHeaders(): void
    {
        $this->conn->subscribe('test.rr.headers', function (Message $msg): void {
            $reqId = $msg->headers?->get('X-Request-Id') ?? 'unknown';
            $msg->respond("id={$reqId}");
        });

        $msg = new Message(
            subject: 'test.rr.headers',
            data: 'test',
            headers: new Headers(['X-Request-Id' => 'req-42']),
        );

        $reply = $this->conn->requestMessage($msg, 2.0);
        self::assertSame('id=req-42', $reply->data);
    }

    public function testRequestTimeout(): void
    {
        // Nitko ne slusa - NATS s no_responders vraca status 503, ili timeout
        $threw = false;
        try {
            $this->conn->request('test.rr.nobody.unique.' . uniqid(), 'hello', 0.5);
        } catch (\Throwable) {
            $threw = true;
        }
        self::assertTrue($threw, 'Expected exception for no responders');
    }

    public function testRequestMessageNoResponders(): void
    {
        $threw = false;
        try {
            $this->conn->requestMessage(
                new Message(subject: 'test.rr.nobody.msg.unique.' . uniqid(), data: 'hello'),
                0.5,
            );
        } catch (\Throwable) {
            $threw = true;
        }
        self::assertTrue($threw, 'Expected exception for no responders on requestMessage');
    }

    public function testMultipleRequestReplies(): void
    {
        $this->conn->subscribe('test.rr.multi', function (Message $msg): void {
            $n = (int) $msg->data;
            $msg->respond((string) ($n * 2));
        });

        for ($i = 1; $i <= 5; $i++) {
            $reply = $this->conn->request('test.rr.multi', (string) $i, 2.0);
            self::assertSame((string) ($i * 2), $reply->data);
        }
    }

    public function testRequestTimeoutCleansUpSubscription(): void
    {
        // Subscribe a silent responder that never replies
        $this->conn->subscribe('test.rr.silent.' . ($id = uniqid()), function (Message $msg): void {
            // intentionally do not respond
        });

        // First request initializes the muxed inbox subscription (+1)
        try {
            $this->conn->request('test.rr.silent.' . $id, 'x', 0.2);
        } catch (\Throwable) {
        }

        $after = $this->conn->numSubscriptions();

        // Second failed request should NOT add more subscriptions
        try {
            $this->conn->request('test.rr.silent.' . $id, 'x', 0.2);
        } catch (\Throwable) {
        }

        self::assertSame($after, $this->conn->numSubscriptions());
    }

    public function testRequestMessageTimeoutCleansUpSubscription(): void
    {
        $this->conn->subscribe('test.rr.silent.msg.' . ($id = uniqid()), function (Message $msg): void {
            // intentionally do not respond
        });

        // First request initializes the muxed inbox subscription (+1)
        try {
            $this->conn->requestMessage(
                new Message(subject: 'test.rr.silent.msg.' . $id, data: 'x'),
                0.2,
            );
        } catch (\Throwable) {
        }

        $after = $this->conn->numSubscriptions();

        // Second failed request should NOT add more subscriptions
        try {
            $this->conn->requestMessage(
                new Message(subject: 'test.rr.silent.msg.' . $id, data: 'x'),
                0.2,
            );
        } catch (\Throwable) {
        }

        self::assertSame($after, $this->conn->numSubscriptions());
    }

    public function testRequestReplyWithQueueGroup(): void
    {
        $handledBy = [];

        $this->conn->queueSubscribe('test.rr.queue', 'workers', function (Message $msg) use (&$handledBy): void {
            $handledBy[] = 'A';
            $msg->respond('from-A');
        });
        $this->conn->queueSubscribe('test.rr.queue', 'workers', function (Message $msg) use (&$handledBy): void {
            $handledBy[] = 'B';
            $msg->respond('from-B');
        });

        $reply = $this->conn->request('test.rr.queue', 'test', 2.0);
        self::assertStringStartsWith('from-', $reply->data);
        self::assertCount(1, $handledBy); // Only one worker responds
    }

    public function testMuxedInboxReusesSingleSubscription(): void
    {
        $this->conn->subscribe('test.rr.mux', function (Message $msg): void {
            $msg->respond('ok');
        });

        // Before any request, no muxed sub exists
        $before = $this->conn->numSubscriptions();

        $reply1 = $this->conn->request('test.rr.mux', 'a', 2.0);
        self::assertSame('ok', $reply1->data);

        // After first request, the muxed sub is created (+1)
        $afterFirst = $this->conn->numSubscriptions();
        self::assertSame($before + 1, $afterFirst);

        $reply2 = $this->conn->request('test.rr.mux', 'b', 2.0);
        self::assertSame('ok', $reply2->data);

        // After second request, no new subscription was added
        self::assertSame($afterFirst, $this->conn->numSubscriptions());
    }

    public function testOldRequestStyleCreatesPerRequestSubscription(): void
    {
        $conn = Connection::connect(self::natsUrl(), new ConnectionOptions(
            name: 'old-style-test-' . uniqid(),
            useOldRequestStyle: true,
        ));

        try {
            $conn->subscribe('test.rr.old', function (Message $msg): void {
                $msg->respond('old-ok');
            });

            $reply = $conn->request('test.rr.old', 'x', 2.0);
            self::assertSame('old-ok', $reply->data);
        } finally {
            $conn->close();
        }
    }

    public function testOldRequestStyleRequestMessage(): void
    {
        $conn = Connection::connect(self::natsUrl(), new ConnectionOptions(
            name: 'old-style-msg-test-' . uniqid(),
            useOldRequestStyle: true,
        ));

        try {
            $conn->subscribe('test.rr.old.msg', function (Message $msg): void {
                $msg->respond('old-msg-ok');
            });

            $reply = $conn->requestMessage(
                new Message(subject: 'test.rr.old.msg', data: 'x'),
                2.0,
            );
            self::assertSame('old-msg-ok', $reply->data);
        } finally {
            $conn->close();
        }
    }

    /**
     * @return list<string>
     */
    private function waitForServerSubscriptions(string $connectionName, int $expectedCount): array
    {
        $deadline = microtime(true) + 1.0;

        do {
            $connz = @file_get_contents('http://127.0.0.1:8222/connz?subs=1');
            self::assertNotFalse($connz, 'Expected NATS monitoring endpoint on port 8222');

            /** @var array{
             *     connections?: list<array{name?: string, subscriptions_list?: list<string>}>
             * } $payload
             */
            $payload = json_decode($connz, true, flags: JSON_THROW_ON_ERROR);

            foreach ($payload['connections'] ?? [] as $connection) {
                if (($connection['name'] ?? null) !== $connectionName) {
                    continue;
                }

                $subscriptions = $connection['subscriptions_list'] ?? [];
                if (count($subscriptions) === $expectedCount) {
                    sort($subscriptions);
                    return $subscriptions;
                }
            }

            usleep(10_000);
        } while (microtime(true) < $deadline);

        self::fail("Timed out waiting for {$connectionName} to have {$expectedCount} server subscriptions");
    }
}
