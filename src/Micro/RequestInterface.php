<?php

declare(strict_types=1);

namespace Sofascore\Nats\Micro;

use Sofascore\Nats\Headers;
use Sofascore\Nats\Message;

interface RequestInterface
{
    public function respond(string $data, ?Headers $headers = null): void;

    public function respondJson(mixed $data, ?Headers $headers = null): void;

    public function error(string $code, string $description, string $data = ''): void;

    public function data(): string;

    public function headers(): ?Headers;

    public function subject(): string;

    public function replyTo(): ?string;

    /**
     * Returns the raw underlying NATS message.
     */
    public function msg(): Message;
}
