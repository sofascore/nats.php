<?php

/**
 * JetStream Consumers - reading messages from streams.
 *
 * Pull consumer: client actively requests messages (fetch).
 * Ordered consumer: guarantees order, auto-recovery.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\AckPolicy;
use Sofascore\Nats\Enum\DeliveryPolicy;
use Sofascore\Nats\JetStream\Consumer\ConsumerConfig;
use Sofascore\Nats\JetStream\Consumer\ConsumeOptions;
use Sofascore\Nats\JetStream\Consumer\FetchOptions;
use Sofascore\Nats\JetStream\Consumer\OrderedConsumerConfig;
use Sofascore\Nats\JetStream\Message\JetStreamMessage;
use Sofascore\Nats\JetStream\Stream\StreamConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// Setup: create stream and publish messages
$stream = $js->createStream(new StreamConfig(
    name: 'EVENTS',
    subjects: ['events.>'],
));

for ($i = 1; $i <= 20; $i++) {
    $js->publish('events.click', json_encode(['id' => $i]));
}

// --- 1. Durable Pull Consumer ---

$consumer = $stream->createOrUpdateConsumer(new ConsumerConfig(
    name: 'click-processor',
    durable: 'click-processor',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    filterSubject: 'events.click',
    maxAckPending: 1000,
));

// --- 2. Fetch - retrieve a batch of messages ---

$batch = $consumer->fetch(5, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    echo "Fetch: {$msg->data()}\n";
    $msg->ack();
}
echo "Batch error: " . ($batch->error() ? $batch->error()->getMessage() : 'none') . "\n";

// --- 3. FetchNoWait - only available messages ---

$batch = $consumer->fetchNoWait(10);
foreach ($batch as $msg) {
    echo "NoWait: {$msg->data()}\n";
    $msg->ack();
}

// --- 4. Next - single message ---

try {
    $msg = $consumer->next(timeout: 2.0);
    echo "Next: {$msg->data()}\n";
    $msg->ack();
} catch (\Sofascore\Nats\TimeoutException) {
    echo "No messages.\n";
}

// --- 5. Messages - iterator-based consumption ---
// This is a blocking generator over repeated fetch() calls.
// It is not started in the background automatically.

$msgs = $consumer->messages(new ConsumeOptions(
    maxMessages: 10,
    expires: 2.0,
));

$count = 0;
foreach ($msgs as $msg) {
    echo "Iterator: {$msg->data()}\n";
    $msg->ack();
    $count++;
    if ($count >= 3) {
        // stop()  — Stops immediately, may skip buffered messages
        // drain() — Processes all buffered messages first, then stops
        $msgs->stop();
    }
}

echo "Messages context closed: " . ($msgs->isClosed() ? 'yes' : 'no') . "\n";

// Poll one message at a time without exception-driven timeout handling.
// If maxMessages is greater than 1, tryNext() drains an internal buffer first.
$poller = $consumer->messages(new ConsumeOptions(
    maxMessages: 1,
    expires: 0.2,
));

while (($msg = $poller->tryNext()) !== null) {
    echo "TryNext: {$msg->data()}\n";
    $msg->ack();
}

// --- 6. Consume - callback pattern ---
// consume() creates a context object; it does not start a background worker.
// You run it explicitly via $ctx->run() from your own process or loop.

$processed = 0;
$js->publish('events.click', json_encode(['id' => 21]));
$js->publish('events.click', json_encode(['id' => 22]));

// &$ctx is bound by reference here so the callback can call drain() on the
// ConsumeContext created by the assignment below.
$ctx = $consumer->consume(function (JetStreamMessage $msg) use (&$processed, &$ctx) {
    echo "Callback: {$msg->data()}\n";
    $msg->ack();
    $processed++;
    if ($processed >= 2) {
        $ctx->drain();
    }
}, new ConsumeOptions(maxMessages: 5, expires: 2.0));

// ConsumeContext methods:
//   $ctx->stop()      — Stop immediately (may leave buffered messages unprocessed)
//   $ctx->drain()     — Process all buffered messages, then stop
//   $ctx->isClosed()  — Has the context stopped?
//   $ctx->lastError() — Last error during consumption (or null)
//   $ctx->info()      — Current ConsumerInfo from the server

echo "Context closed before run: " . ($ctx->isClosed() ? 'yes' : 'no') . "\n";
echo "Last error: " . ($ctx->lastError() ? $ctx->lastError()->getMessage() : 'none') . "\n";

// Fetch current consumer state from server while consuming
$consumerInfo = $ctx->info();
echo "Consumer pending: {$consumerInfo->numPending}\n";

// Run the callback loop in the foreground until drain() stops it.
$ctx->run();
echo "Processed via consume(): {$processed}\n";

// --- 7. ACK variants ---

$batch = $consumer->fetch(1, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    // $msg->ack();                    // Explicit ACK
    // $msg->ackSync(timeout: 5.0);    // Sync ACK (waits for confirmation)
    // $msg->nak();                    // Negative ACK (redelivery)
    // $msg->nakWithDelay(5.0);        // NAK with 5s delay
    // $msg->term();                   // Terminal - don't retry
    // $msg->termWithReason('bad data');
    // $msg->inProgress();             // Reset ack timer

    $metadata = $msg->metadata();
    echo "Stream seq: {$metadata->streamSequence}\n";
    echo "Consumer seq: {$metadata->consumerSequence}\n";
    echo "Pending: {$metadata->numPending}\n";
    $msg->ack();
}

// --- 8. Ordered Consumer (auto-recovery) ---

$ordered = $stream->orderedConsumer(new OrderedConsumerConfig(
    filterSubject: 'events.click',
    deliverPolicy: DeliveryPolicy::All,
));

$batch = $ordered->fetch(5);
foreach ($batch as $msg) {
    echo "Ordered: {$msg->data()}\n";
}

// Cleanup
$js->deleteStream('EVENTS');
$conn->close();
