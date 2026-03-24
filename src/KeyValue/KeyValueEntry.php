<?php

declare(strict_types=1);

namespace Sofascore\Nats\KeyValue;

use Sofascore\Nats\Enum\KeyValueOperation;

readonly class KeyValueEntry
{
    public function __construct(
        public string $key,
        public string $value,
        public int $revision,
        public int $delta,
        public KeyValueOperation $operation,
        public \DateTimeImmutable $timestamp,
        public string $bucket,
    ) {}
}
