<?php

declare(strict_types=1);

namespace Sofascore\Nats\Micro;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\ServiceVerb;
use Sofascore\Nats\Inbox;
use Sofascore\Nats\Message;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\Subscription;

final class Service implements ServiceInterface
{
    /**
     * Build a control subject string for service discovery.
     * Matches Go client micro.ControlSubject().
     *
     * Examples:
     *   controlSubject(ServiceVerb::Ping) => '$SRV.PING'
     *   controlSubject(ServiceVerb::Info, 'myservice') => '$SRV.INFO.myservice'
     *   controlSubject(ServiceVerb::Stats, 'myservice', 'abc123') => '$SRV.STATS.myservice.abc123'
     */
    public static function controlSubject(ServiceVerb $verb, string $name = '', string $id = ''): string
    {
        $subject = '$SRV.' . $verb->value;
        if ($name !== '') {
            $subject .= '.' . $name;
            if ($id !== '') {
                $subject .= '.' . $id;
            }
        }
        return $subject;
    }

    private readonly string $id;
    private \DateTimeImmutable $started;
    private bool $stopped = false;
    private ?\Throwable $lastError = null;
    private int $handlerDepth = 0;
    private bool $pendingStopCleanup = false;

    /** @var array<string, array{handler: \Closure|HandlerInterface, config: EndpointConfig, sub: Subscription, stats: array{requests: int, errors: int, processing_time: float, last_error: ?string}}> */
    private array $endpoints = [];

    /** @var list<Subscription> Control subscriptions */
    private array $controlSubs = [];

    private function __construct(
        private readonly Connection $connection,
        private readonly ServiceConfig $config,
    ) {
        $this->id = Inbox::nuid();
        $this->started = new \DateTimeImmutable();
    }

    public static function create(Connection $conn, ServiceConfig $config): self
    {
        $service = new self($conn, $config);
        $service->setupControlSubjects();
        return $service;
    }

    public function addEndpoint(
        string $name,
        \Closure|HandlerInterface $handler,
        ?EndpointConfig $config = null,
    ): self {
        if ($this->stopped) {
            throw new NatsException('Service is stopped');
        }

        $config ??= new EndpointConfig();
        $subject = $this->subjectFor($name, $config);
        $queueGroup = $this->queueGroupFor($config);

        $wrappedHandler = function (Message $msg) use ($name, $handler): void {
            $request = new Request($msg, $this->connection);
            $startTime = hrtime(true);
            $this->handlerDepth++;

            try {
                if ($handler instanceof HandlerInterface) {
                    $handler->handle($request);
                } else {
                    $handler($request);
                }

                $elapsed = (hrtime(true) - $startTime) / 1_000_000_000;
                $stats = $this->endpoints[$name]['stats'];
                $stats['requests']++;
                $stats['processing_time'] += $elapsed;
                $this->endpoints[$name]['stats'] = $stats;
            } catch (\Throwable $e) {
                $elapsed = (hrtime(true) - $startTime) / 1_000_000_000;
                $stats = $this->endpoints[$name]['stats'];
                $stats['requests']++;
                $stats['errors']++;
                $stats['processing_time'] += $elapsed;
                $stats['last_error'] = $e->getMessage();
                $this->endpoints[$name]['stats'] = $stats;

                $this->recordError($e);

                // Send error response if not already responded
                if (!$request->hasResponded()) {
                    try {
                        $request->error('500', $e->getMessage());
                    } catch (\Throwable $sendError) {
                        $this->recordError($sendError);
                    }
                }
            } finally {
                $this->handlerDepth--;
                $this->finishPendingStopCleanup();
            }
        };

        $sub = $queueGroup !== null
            ? $this->connection->queueSubscribe($subject, $queueGroup, $wrappedHandler)
            : $this->connection->subscribe($subject, $wrappedHandler);

        // Apply pending limits if configured
        if ($config->pendingMsgLimit !== null || $config->pendingBytesLimit !== null) {
            $sub->setPendingLimits(
                $config->pendingMsgLimit ?? 65536,
                $config->pendingBytesLimit ?? 67_108_864,
            );
        }

        $this->endpoints[$name] = [
            'handler' => $handler,
            'config' => $config,
            'sub' => $sub,
            'stats' => [
                'requests' => 0,
                'errors' => 0,
                'processing_time' => 0.0,
                'last_error' => null,
            ],
        ];

        return $this;
    }

    public function addGroup(string $name, ?string $queueGroup = null, bool $queueGroupDisabled = false): GroupInterface
    {
        if ($this->stopped) {
            throw new NatsException('Service is stopped');
        }

        return new Group($this, $name, $queueGroup, $queueGroupDisabled);
    }

    public function info(): ServiceInfo
    {
        /** @var list<array{name: string, subject: string, metadata?: array<string, string>}> $endpoints */
        $endpoints = [];
        foreach ($this->endpoints as $name => $ep) {
            $subject = $this->subjectFor($name, $ep['config']);
            if ($ep['config']->metadata !== []) {
                $endpoints[] = [
                    'name' => $name,
                    'subject' => $subject,
                    'metadata' => $ep['config']->metadata,
                ];
            } else {
                $endpoints[] = [
                    'name' => $name,
                    'subject' => $subject,
                ];
            }
        }

        return new ServiceInfo(
            identity: new ServiceIdentity(
                name: $this->config->name,
                id: $this->id,
                version: $this->config->version,
                metadata: $this->config->metadata,
            ),
            description: $this->config->description,
            endpoints: $endpoints,
        );
    }

    public function stats(): ServiceStats
    {
        $endpointStats = [];
        foreach ($this->endpoints as $name => $ep) {
            $totalReqs = $ep['stats']['requests'];
            $totalTime = $ep['stats']['processing_time'];
            $avgTime = $totalReqs > 0 ? $totalTime / $totalReqs : 0;

            $epStats = new EndpointStats(
                name: $name,
                subject: $this->subjectFor($name, $ep['config']),
                numRequests: $totalReqs,
                numErrors: $ep['stats']['errors'],
                processingTime: $totalTime,
                averageProcessingTime: $avgTime,
                lastError: $ep['stats']['last_error'],
            );

            // Invoke custom stats handler if configured (matches Go StatsHandler)
            if ($this->config->statsHandler !== null) {
                $customData = ($this->config->statsHandler)($epStats);
                $epStats = new EndpointStats(
                    name: $epStats->name,
                    subject: $epStats->subject,
                    numRequests: $epStats->numRequests,
                    numErrors: $epStats->numErrors,
                    processingTime: $epStats->processingTime,
                    averageProcessingTime: $epStats->averageProcessingTime,
                    lastError: $epStats->lastError,
                    data: $customData,
                );
            }

            $endpointStats[] = $epStats;
        }

        return new ServiceStats(
            identity: new ServiceIdentity(
                name: $this->config->name,
                id: $this->id,
                version: $this->config->version,
                metadata: $this->config->metadata,
            ),
            started: $this->started,
            endpoints: $endpointStats,
            metadata: $this->config->metadata,
        );
    }

    public function reset(): void
    {
        foreach ($this->endpoints as &$ep) {
            $ep['stats'] = [
                'requests' => 0,
                'errors' => 0,
                'processing_time' => 0.0,
                'last_error' => null,
            ];
        }

        $this->lastError = null;
        $this->started = new \DateTimeImmutable();
    }

    public function stop(): void
    {
        if ($this->stopped) {
            return;
        }

        $this->stopped = true;
        $subs = $this->serviceSubscriptions();

        if ($this->connection->isConnected()) {
            foreach ($subs as $sub) {
                $sub->drain();
            }

            if ($this->handlerDepth === 0) {
                try {
                    $this->connection->flush(0.5);
                    $this->finalizeStoppedSubscriptions($subs);
                } catch (\Throwable $e) {
                    $this->recordError($e);
                    foreach ($subs as $sub) {
                        $sub->unsubscribe();
                    }
                }
            } else {
                $this->pendingStopCleanup = true;
            }
        } else {
            foreach ($subs as $sub) {
                $sub->unsubscribe();
            }

            if (!$this->connection->isClosed()) {
                $this->recordError(new NatsException(
                    'Service stopped without graceful drain because the connection was not connected',
                ));
            }
        }

        $doneHandler = $this->config->doneHandler;
        if ($doneHandler !== null) {
            $doneHandler($this);
        }
    }

    public function isStopped(): bool
    {
        return $this->stopped;
    }

    public function lastError(): ?\Throwable
    {
        return $this->lastError;
    }

    public function id(): string
    {
        return $this->id;
    }

    public function name(): string
    {
        return $this->config->name;
    }

    public function version(): string
    {
        return $this->config->version;
    }

    // --- Internal: control subjects ---

    private function setupControlSubjects(): void
    {
        $name = $this->config->name;
        $id = $this->id;

        // $SRV.PING, $SRV.PING.<name>, $SRV.PING.<name>.<id>
        $this->controlSubs[] = $this->connection->subscribe('$SRV.PING', $this->handlePing(...));
        $this->controlSubs[] = $this->connection->subscribe("\$SRV.PING.{$name}", $this->handlePing(...));
        $this->controlSubs[] = $this->connection->subscribe("\$SRV.PING.{$name}.{$id}", $this->handlePing(...));

        // $SRV.INFO
        $this->controlSubs[] = $this->connection->subscribe('$SRV.INFO', $this->handleInfo(...));
        $this->controlSubs[] = $this->connection->subscribe("\$SRV.INFO.{$name}", $this->handleInfo(...));
        $this->controlSubs[] = $this->connection->subscribe("\$SRV.INFO.{$name}.{$id}", $this->handleInfo(...));

        // $SRV.STATS
        $this->controlSubs[] = $this->connection->subscribe('$SRV.STATS', $this->handleStats(...));
        $this->controlSubs[] = $this->connection->subscribe("\$SRV.STATS.{$name}", $this->handleStats(...));
        $this->controlSubs[] = $this->connection->subscribe("\$SRV.STATS.{$name}.{$id}", $this->handleStats(...));
    }

    private function handlePing(Message $msg): void
    {
        $response = [
            'type' => 'io.nats.micro.v1.ping_response',
            'name' => $this->config->name,
            'id' => $this->id,
            'version' => $this->config->version,
        ];
        if ($this->config->metadata !== []) {
            $response['metadata'] = $this->config->metadata;
        }

        $this->replyControl($msg, $response);
    }

    private function handleInfo(Message $msg): void
    {
        $this->replyControl($msg, $this->info()->toArray());
    }

    private function handleStats(Message $msg): void
    {
        $this->replyControl($msg, $this->stats()->toArray());
    }

    /**
     * @return list<Subscription>
     */
    private function serviceSubscriptions(): array
    {
        $subs = [];
        foreach ($this->endpoints as $ep) {
            $subs[] = $ep['sub'];
        }

        return array_merge($subs, $this->controlSubs);
    }

    private function subjectFor(string $name, EndpointConfig $config): string
    {
        return $config->subject ?? $name;
    }

    private function queueGroupFor(EndpointConfig $config): ?string
    {
        return match (true) {
            $config->queueGroupDisabled => null,
            $config->queueGroup !== null => $config->queueGroup,
            $this->config->queueGroupDisabled => null,
            $this->config->queueGroup !== null => $this->config->queueGroup,
            default => 'q',
        };
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function replyControl(Message $msg, array $payload): void
    {
        $this->handlerDepth++;

        try {
            $msg->respond(json_encode($payload, JSON_THROW_ON_ERROR));
        } catch (\Throwable $e) {
            $this->recordError($e);
        } finally {
            $this->handlerDepth--;
            $this->finishPendingStopCleanup();
        }
    }

    private function recordError(\Throwable $e): void
    {
        $this->lastError = $e;

        $errorHandler = $this->config->errorHandler;
        if ($errorHandler !== null) {
            $errorHandler($this, $e);
        }
    }

    private function finishPendingStopCleanup(): void
    {
        if (!$this->pendingStopCleanup || $this->handlerDepth !== 0) {
            return;
        }

        $this->pendingStopCleanup = false;
        $subs = $this->serviceSubscriptions();

        if ($this->connection->isConnected()) {
            try {
                $this->connection->flush(0.5);
            } catch (\Throwable $e) {
                $this->recordError($e);
                foreach ($subs as $sub) {
                    $sub->unsubscribe();
                }
                return;
            }
        }

        $this->finalizeStoppedSubscriptions($subs);
    }

    /**
     * @param list<Subscription> $subs
     */
    private function finalizeStoppedSubscriptions(array $subs): void
    {
        foreach ($subs as $sub) {
            $sub->checkDrainComplete();
        }

        foreach ($subs as $sub) {
            if (!$sub->isDrained() && $sub->isValid()) {
                $sub->unsubscribe();
            }
        }
    }
}
