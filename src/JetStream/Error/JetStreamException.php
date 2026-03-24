<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Error;

use Sofascore\Nats\NatsException;
use Sofascore\Nats\Internal\TypeCast as T;

class JetStreamException extends NatsException
{
    public function __construct(
        string $message,
        public readonly ?ApiError $apiError = null,
        int $code = 0,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromApiResponse(array $data): self
    {
        $error = $data['error'] ?? null;
        if ($error !== null) {
            $apiError = ApiError::fromArray(T::stringKeyArray($error));
            return new self($apiError->description, $apiError, $apiError->code);
        }
        return new self('Unknown JetStream error');
    }
}
