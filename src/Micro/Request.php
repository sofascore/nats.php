<?php

declare(strict_types=1);

namespace Sofascore\Nats\Micro;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Headers;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;

final class Request implements RequestInterface
{
    private bool $responded = false;
    private readonly Message $msg;
    private readonly Connection $connection;

    public function __construct(Message $msg, Connection $connection)
    {
        $this->connection = $connection;
        $this->msg = new Message(
            subject: $msg->subject,
            data: $msg->data,
            replyTo: $msg->replyTo,
            headers: $msg->headers,
            connection: $connection,
            beforeRespond: $this->assertCanRespond(...),
            afterRespond: $this->markResponded(...),
        );
    }

    public function respond(string $data, ?Headers $headers = null): void
    {
        $this->sendResponse($data, $headers);
    }

    public function respondJson(mixed $data, ?Headers $headers = null): void
    {
        $headers ??= new Headers();
        $headers->set('Content-Type', 'application/json');
        $this->respond(json_encode($data, JSON_THROW_ON_ERROR), $headers);
    }

    public function error(string $code, string $description, string $data = ''): void
    {
        $headers = new Headers();
        $headers->set('Nats-Service-Error', $description);
        $headers->set('Nats-Service-Error-Code', $code);

        $this->sendResponse($data, $headers);
    }

    public function hasResponded(): bool
    {
        return $this->responded;
    }

    public function data(): string
    {
        return $this->msg->data;
    }

    public function headers(): ?Headers
    {
        return $this->msg->headers;
    }

    public function subject(): string
    {
        return $this->msg->subject;
    }

    public function replyTo(): ?string
    {
        return $this->msg->replyTo;
    }

    public function msg(): Message
    {
        return $this->msg;
    }

    private function sendResponse(string $data, ?Headers $headers = null): void
    {
        if ($this->msg->replyTo === null) {
            throw new NatsException('No reply subject');
        }

        $this->assertCanRespond();

        $response = new Message(
            subject: $this->msg->replyTo,
            data: $data,
            headers: $headers,
        );
        $this->connection->publishMessage($response);
        $this->markResponded();
    }

    private function assertCanRespond(): void
    {
        if ($this->responded) {
            throw new NatsException('Response already sent');
        }
    }

    private function markResponded(): void
    {
        $this->responded = true;
    }
}
