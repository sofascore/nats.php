<?php

declare(strict_types=1);

namespace Sofascore\Nats\JetStream\Consumer;

use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\Enum\ReplayPolicy;

readonly class OrderedConsumerConfig
{
    public function __construct(
        public ?string $filterSubject = null,
        /** @var list<string> */
        public array $filterSubjects = [],
        public DeliveryPolicy $deliverPolicy = DeliveryPolicy::All,
        public ?int $optStartSeq = null,
        public ?\DateTimeImmutable $optStartTime = null,
        public ReplayPolicy $replayPolicy = ReplayPolicy::Instant,
        public bool $headersOnly = false,
        public ?float $maxResetAttempts = null,
    ) {}
}
