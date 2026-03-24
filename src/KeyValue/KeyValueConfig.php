<?php

declare(strict_types=1);

namespace Sofascore\Nats\KeyValue;

use Sofascore\Nats\JetStream\Stream\Placement;
use Sofascore\Nats\JetStream\Stream\RePublish;
use Sofascore\Nats\JetStream\Stream\StreamSource;

readonly class KeyValueConfig
{
    public function __construct(
        public string $bucket,
        public ?string $description = null,
        public ?int $maxBytes = null,
        public int $history = 1,
        public ?float $ttl = null,
        public ?int $maxValueSize = null,
        public int $replicas = 1,
        public ?Placement $placement = null,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?StreamSource $mirror = null,
        /** @var list<StreamSource> */
        public array $sources = [],
        public bool $compression = false,
        public ?RePublish $rePublish = null,
        public ?float $limitMarkerTtl = null,
        public bool $allowMsgTtl = false,
    ) {}
}
