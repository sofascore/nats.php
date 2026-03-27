<?php

declare(strict_types=1);

namespace Sofascore\Nats\Tests\Integration\KeyValue;

use Sofascore\Nats\Connection;
use Sofascore\Nats\Enum\KeyValueOperation;
use Sofascore\Nats\JetStream\JetStreamContext;
use Sofascore\Nats\KeyValue\KeyValueConfig;
use Sofascore\Nats\NatsException;
use Sofascore\Nats\Tests\Integration\NatsUrlTrait;
use PHPUnit\Framework\TestCase;

final class KeyValueExtendedTest extends TestCase
{
    use NatsUrlTrait;

    private Connection $conn;
    private JetStreamContext $js;
    private string $bucket;
    private string $bucket2;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::natsUrl());
        $this->js = $this->conn->jetStream();
        $this->bucket = 'test-kvext-' . substr(uniqid(), -6);
        $this->bucket2 = 'test-kvext2-' . substr(uniqid(), -6);

        try { $this->js->deleteKeyValue($this->bucket); } catch (\Throwable) {}
        try { $this->js->deleteKeyValue($this->bucket2); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteKeyValue($this->bucket); } catch (\Throwable) {}
        try { $this->js->deleteKeyValue($this->bucket2); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testGetRevision(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $rev1 = $kv->put('counter', 'one');
        $rev2 = $kv->put('counter', 'two');
        $rev3 = $kv->put('counter', 'three');

        // Get specific revision
        $entry1 = $kv->getRevision('counter', $rev1);
        self::assertSame('one', $entry1->value);
        self::assertSame($rev1, $entry1->revision);
        self::assertSame('counter', $entry1->key);

        $entry2 = $kv->getRevision('counter', $rev2);
        self::assertSame('two', $entry2->value);
        self::assertSame($rev2, $entry2->revision);

        $entry3 = $kv->getRevision('counter', $rev3);
        self::assertSame('three', $entry3->value);
    }

    public function testGetRevisionWrongKey(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('alpha', 'value-a');
        $rev2 = $kv->put('beta', 'value-b');

        // Trying to get revision for 'alpha' but the revision belongs to 'beta'
        $this->expectException(NatsException::class);
        $kv->getRevision('alpha', $rev2);
    }

    public function testListKeysFiltered(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('a.b.c', 'val1');
        $kv->put('a.b.d', 'val2');
        $kv->put('a.b.e', 'val3');
        $kv->put('x.y.z', 'val4');
        $kv->put('x.y.w', 'val5');

        // Filter for a.b.* keys
        $filteredKeys = [];
        foreach ($kv->listKeysFiltered('a.b.*') as $key) {
            $filteredKeys[] = $key;
        }

        sort($filteredKeys);
        self::assertCount(3, $filteredKeys);
        self::assertSame(['a.b.c', 'a.b.d', 'a.b.e'], $filteredKeys);
    }

    public function testListKeysFilteredMultiplePatterns(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('a.b.c', 'val1');
        $kv->put('x.y.z', 'val2');
        $kv->put('m.n.o', 'val3');

        // Filter with multiple patterns
        $filteredKeys = [];
        foreach ($kv->listKeysFiltered('a.b.*', 'x.y.*') as $key) {
            $filteredKeys[] = $key;
        }

        sort($filteredKeys);
        self::assertCount(2, $filteredKeys);
        self::assertContains('a.b.c', $filteredKeys);
        self::assertContains('x.y.z', $filteredKeys);
    }

    public function testCreateOrUpdateKeyValue(): void
    {
        // Create bucket via createOrUpdate
        $kv = $this->js->createOrUpdateKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 3,
        ));

        $kv->put('key1', 'val1');
        $status = $kv->status();
        self::assertSame(3, $status->history);

        // Update with different config
        $kv2 = $this->js->createOrUpdateKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 3,
            description: 'Updated bucket',
        ));

        $status2 = $kv2->status();
        self::assertSame('Updated bucket', $status2->config->description);

        // Data should still be there
        $entry = $kv2->get('key1');
        self::assertSame('val1', $entry->value);
    }

    public function testKeyValueStoreNames(): void
    {
        $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 1,
        ));
        $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket2,
            history: 1,
        ));

        $names = [];
        foreach ($this->js->keyValueStoreNames() as $name) {
            $names[] = $name;
        }

        self::assertContains($this->bucket, $names);
        self::assertContains($this->bucket2, $names);
    }

    public function testCreateWithKeyTtl(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
            allowMsgTtl: true,
        ));

        // Create a key with TTL (the TTL header is set but verifying expiry is hard)
        $rev = $kv->create('ttl-key', 'ttl-value', keyTtl: 3600.0);
        self::assertGreaterThan(0, $rev);

        // Verify we can still read the key immediately
        $entry = $kv->get('ttl-key');
        self::assertSame('ttl-value', $entry->value);
        self::assertSame($rev, $entry->revision);
    }

    public function testLimitMarkerTtlIsAppliedToBackingStreamConfigAndStatus(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
            allowMsgTtl: true,
            limitMarkerTtl: 600.0,
        ));

        $status = $kv->status();

        self::assertSame(600.0, $status->limitMarkerTtl);
        self::assertSame(600.0, $status->config->limitMarkerTtl);
        self::assertTrue($status->config->allowMsgTtl);
        self::assertSame(600, $status->streamInfo?->config->subjectDeleteMarkerTtl);
    }

    public function testPurgeDeletesPreservesLiveKeys(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('alive', 'value1');
        $kv->put('also-alive', 'value2');
        $kv->put('to-delete', 'gone');
        $kv->delete('to-delete');

        // Use negative TTL to always remove markers
        $kv->purgeDeletes(-1.0);

        // Live keys must still be present with full history
        self::assertSame('value1', $kv->get('alive')->value);
        self::assertSame('value2', $kv->get('also-alive')->value);
        self::assertCount(1, $kv->history('alive'));
        self::assertCount(1, $kv->history('also-alive'));

        // Deleted key's tombstone should be fully purged (empty history)
        $this->expectException(NatsException::class);
        $kv->get('to-delete');
    }

    public function testPurgeDeletesRemovesPurgedKeys(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('alive', 'keeper');
        $kv->put('to-purge', 'v1');
        $kv->put('to-purge', 'v2');
        $kv->purge('to-purge');

        // Use negative TTL to always remove markers
        $kv->purgeDeletes(-1.0);

        // Live key preserved
        self::assertSame('keeper', $kv->get('alive')->value);

        // Purged key's tombstone should be removed
        $this->expectException(NatsException::class);
        $kv->get('to-purge');
    }

    public function testPurgeDeletesOnlyDeletedNotReput(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        // Delete a key then re-put it — it should survive purgeDeletes
        $kv->put('revived', 'first');
        $kv->delete('revived');
        $kv->put('revived', 'back');

        $kv->purgeDeletes(-1.0);

        self::assertSame('back', $kv->get('revived')->value);
        self::assertCount(3, $kv->history('revived'));
    }

    public function testPurgeDeletesRecentMarkerKeepsOneWithDefaultTtl(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('key', 'v1');
        $kv->put('key', 'v2');
        $kv->delete('key');

        // Default TTL is 30 min — the just-created tombstone is newer,
        // so purgeDeletes should keep=1 (preserve the tombstone, reclaim old revisions)
        $kv->purgeDeletes();

        $history = $kv->history('key');
        self::assertCount(1, $history);
        self::assertSame(KeyValueOperation::Delete, $history[0]->operation);
    }

    public function testPurgeDeletesNegativeTtlAlwaysRemovesMarker(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
        ));

        $kv->put('key', 'v1');
        $kv->put('key', 'v2');
        $kv->delete('key');

        // Negative TTL: always remove marker entirely
        $kv->purgeDeletes(-1.0);

        $this->expectException(NatsException::class);
        $kv->get('key');
    }

    public function testPurgeWithTtl(): void
    {
        $kv = $this->js->createKeyValue(new KeyValueConfig(
            bucket: $this->bucket,
            history: 5,
            allowMsgTtl: true,
        ));

        $kv->put('purge-key', 'v1');
        $kv->put('purge-key', 'v2');
        $kv->put('purge-key', 'v3');

        // Purge with TTL parameter - verify no error
        $kv->purge('purge-key', ttl: 3600.0);

        // Key should no longer be found
        $this->expectException(NatsException::class);
        $kv->get('purge-key');
    }
}
