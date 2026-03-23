<?php

/**
 * Key-Value Watch - change tracking with a generator-based watcher.
 *
 * Watcher tracks changes on keys and yields KeyValueEntry objects.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\KeyValue\KeyValueConfig;
use Nats\KeyValue\WatchOptions;

$conn = Connection::connect();
$js = $conn->jetStream();

$kv = $js->createKeyValue(new KeyValueConfig(
    bucket: 'config',
    history: 10,
));

// --- Watch all keys ---

$watcher = $kv->watch(
    '>',                                // All keys
    WatchOptions::updatesOnly(),        // Only new changes (not initial)
    WatchOptions::ignoreDeletes(),      // Ignore deletions
);

// In another process or loop iteration someone would do put/delete...
// $kv->put('db.host', 'localhost');
// $kv->put('db.port', '5432');

// watch() returns an iterator-style watcher.
// Iteration blocks while waiting for updates; there is no background worker here.
// foreach ($watcher as $entry) {
//     echo "Changed: {$entry->key} = {$entry->value} (rev {$entry->revision})\n";
//     if ($shouldStop) {
//         $watcher->stop();
//     }
// }

// --- Watch specific pattern ---

// $watcher = $kv->watch('db.*');

// --- Watch with history ---

// $watcher = $kv->watch('>', WatchOptions::includeHistory());

// --- Watch with resume from revision ---

// $watcher = $kv->watch('>', WatchOptions::resumeFromRevision(42));

$js->deleteKeyValue('config');
$conn->close();
