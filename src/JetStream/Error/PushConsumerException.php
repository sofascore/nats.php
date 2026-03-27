<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Error;

class PushConsumerException extends JetStreamException
{
    public function __construct(
        string $message,
        public readonly int $statusCode,
        int $code = 0,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, code: $code, previous: $previous);
    }
}
