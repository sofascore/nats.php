<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\Micro;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Headers;
use Sofascore\Nats\Message;
use Sofascore\Nats\Micro\EndpointConfig;
use Sofascore\Nats\Micro\RequestInterface;
use Sofascore\Nats\Micro\Service;
use Sofascore\Nats\Micro\ServiceConfig;
use Sofascore\Nats\Tests\Integration\NatsUrlTrait;
use PHPUnit\Framework\TestCase;

final class ServiceExtendedTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
    }

    protected function tearDown(): void
    {
        $this->conn->close();
    }

    public function testServiceName(): void
    {
        $svcName = 'name-test-' . substr(uniqid(), -6);

        $svc = Service::create($this->conn, new ServiceConfig(
            name: $svcName,
            version: '1.0.0',
        ));

        self::assertSame($svcName, $svc->name());

        $svc->stop();
    }

    public function testServiceVersion(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'version-test-' . substr(uniqid(), -6),
            version: '2.5.3',
        ));

        self::assertSame('2.5.3', $svc->version());

        $svc->stop();
    }

    public function testServiceId(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'id-test-' . substr(uniqid(), -6),
            version: '1.0.0',
        ));

        $id = $svc->id();
        self::assertIsString($id);
        self::assertNotEmpty($id);

        // Create a second service and verify IDs are unique
        $svc2 = Service::create($this->conn, new ServiceConfig(
            name: 'id-test2-' . substr(uniqid(), -6),
            version: '1.0.0',
        ));

        self::assertNotSame($svc->id(), $svc2->id());

        $svc->stop();
        $svc2->stop();
    }

    public function testRequestExposesInboundMetadataWithoutRawMessage(): void
    {
        $svcName = 'reqmeta-test-' . substr(uniqid(), -6);
        $subject = 'svc.reqmeta.' . substr(uniqid(), -6);

        $capturedData = null;
        $capturedSubject = null;
        $capturedReplyTo = null;
        $capturedHeader = null;

        $svc = Service::create($this->conn, new ServiceConfig(
            name: $svcName,
            version: '1.0.0',
        ));

        $svc->addEndpoint('echo', function (RequestInterface $req) use (
            &$capturedData,
            &$capturedSubject,
            &$capturedReplyTo,
            &$capturedHeader,
        ): void {
            $capturedData = $req->data();
            $capturedSubject = $req->subject();
            $capturedReplyTo = $req->replyTo();
            $capturedHeader = $req->headers()?->get('X-Request-Id');

            $req->respond('got: ' . $req->data());
        }, new EndpointConfig(subject: $subject));

        $reply = $this->conn->requestMessage(new Message(
            subject: $subject,
            data: 'hello-world',
            headers: new Headers(['X-Request-Id' => 'req-42']),
        ), 2.0);

        self::assertSame('got: hello-world', $reply->data);
        self::assertSame('hello-world', $capturedData);
        self::assertSame($subject, $capturedSubject);
        self::assertNotNull($capturedReplyTo, 'Request should expose the reply subject');
        self::assertSame('req-42', $capturedHeader);

        $svc->stop();
    }
}
