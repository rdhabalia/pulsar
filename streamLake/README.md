# Streaming Lake — reference engine + end-to-end test

A runnable, dependency-free Java implementation of the **Streaming Lake** design
(Pulsar + BookKeeper lakehouse) agreed in the design review. It models the exact
component boundaries of the real system so the full pipeline can be exercised and
asserted end-to-end **today**, before forking the Pulsar/BookKeeper codebases.

```bash
./run.sh        # compiles with javac and runs the end-to-end test (Java 21+)
```

Expected tail:
```
ScanMetrics{totalPages=30, prunedByDate=10, prunedByRange=14, pagesRead=6, rowsReturned=48}
PASS  end-to-end pruned scan (deptId>1 AND (salary<50 OR salary>100))
PASS  page-index recovery rebuilds from sidecars
ALL TESTS PASSED ✅
```

## What it proves

The test (`StreamingLakeEndToEndTest`) creates a `persons` topic
(`name`, `departmentId`, `salary`), publishes **240 records across 3 days**, which
produces **3 date partitions, 12 ledgers, 30 columnar pages**, then a consumer scans a
2-day window for `departmentId > 1 AND (salary < 50 OR salary > 100)` and asserts:

| Flow | Assertion |
|------|-----------|
| **Date-partitioned ledger** | DateIndexLedger has 3 partitions; every day maps to ledgers |
| **Multiple ledgers / multiple pages** | 12 ledgers, rolled at 3 pages/ledger; many hold >1 page |
| **Columnar pub-sub decode** | page decodes back to its rows; pushdown == filter(full-decode) |
| **Date pruning** | day-2 ledgers skipped → 10 pages pruned by date |
| **Bookie range pruning** | byte-only range overlap → 14 pages pruned by range |
| **Page reads** | only **6 of 30** pages fetched + decoded |
| **Row-level pushdown** | Vortex/columnar decodes only matching rows → 48 records |
| **Correctness** | result is identical to a brute-force oracle; every row satisfies the predicate |
| **Recovery (concern ②)** | wipe + rebuild the page-range index from sidecars → identical result |

## How the three design concerns are satisfied

1. **Columnar storage** — `ColumnarPageCodec` transposes rows into a self-describing
   columnar page (version + flag + column directory + column blocks).
2. **Bookie stays schema-agnostic** — the broker ships an opaque, **order-preserving**
   range blob (`ColumnRange`); `Bookie.giveIndexPages` prunes using only
   `ColumnRange.overlaps` (lexicographic `byte[]` compare). The bookie never decodes a
   payload, learns a type/name, or runs a query.
3. **No EntryStats mutation** — the digested payload is the columnar `byte[]`; ranges
   ride alongside as a separate sidecar and are persisted with the entry so the index is
   recoverable without parsing the payload.

## Mapping to the real Pulsar / BookKeeper code (integration path)

| This module | Real Pulsar/BookKeeper touchpoint (verified) |
|-------------|-----------------------------------------------|
| `StreamingLakeConfig` | new field on `TopicPolicies` (`pulsar-common`) + `createStreamingLakeTopic` admin |
| `StreamingLakeBroker.publish` / buffering | broker publish path: `ServerCnx.handleSend` → `Producer.publishMessageToTopic` → `PersistentTopic.publishMessage` → `ManagedLedger.asyncAddEntry` |
| `EntryStats` / `computeRanges` | computed at `handleSend` after `Commands.parseMessageMetadata`; carried via `MessagePublishContext` |
| `DateIndexLedger` + rollover | hook `ManagedLedgerImpl.ledgerClosed` / `createLedgerAfterClosed`; dedicated append-only ledger |
| `CommandScan` / `StreamingLakeConsumer.scan` | new `CommandScan` in `PulsarApi.proto` + `BaseCommand.Type`; dispatch in `PulsarDecoder`; `ServerCnx.handleScan`; client via `PulsarClientImpl.newConsumer` family |
| `Bookie.addEntry` + range sidecar | `SingleDirectoryDbLedgerStorage.addEntry` / `checkpoint`; new optional field on BK `AddRequest` (`BookkeeperProtocol.proto`) |
| `Bookie` page-range index | new RocksDB CF `ledger-page-index` modeled on `EntryLocationIndex` / `KeyValueStorageRocksDB` |
| `Bookie.giveIndexPages` | new `PAGE_PRUNE` op in `BookkeeperProtocol.proto` + processor in `BookieRequestProcessor` |
| `Bookie.recoverPageIndex` | rebuild on startup modeled on `LocationsIndexRebuildOp` |
| `ColumnarPageCodec` (`PageCodec`) | JVM-native codec; swap a Vortex (JNI) `PageCodec` impl later — interface is the seam |

## Deliberately modeled (not yet wired to the real cluster)

- In-memory `Bookie` instead of RocksDB + entry log on disk.
- Page codec is JVM-native (the agreed Arrow/Parquet-style fallback); Vortex JNI is a
  drop-in `PageCodec` once its JVM binding is confirmed.
- Broker buffering is single-threaded and in-process; the production version needs the
  **transient ingest-journal durability** path (the decision from the review) and
  backpressure. This is scoped to the *Streaming-Lake topic type* only.

See `docs/DESIGN_IMPL.md` for the source design (the doc's "Design-impl" tab).
