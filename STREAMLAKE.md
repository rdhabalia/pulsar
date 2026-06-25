# Streaming Lake (StreamLake) on Apache Pulsar + BookKeeper

StreamLake turns a Pulsar topic into a **columnar, predicate‑prunable store** while keeping
normal pub/sub working. Messages are batched into **column‑major pages**, the **bookie** keeps a
small **page‑index** of per‑column min/max ranges (without understanding the schema), and an
analytical query prunes data at three levels — **date partition → bookie column ranges → row
filter** — so only the matching pages are read and only the matching rows are decoded.

This document covers:

1. [How it works — end‑to‑end flow (the 13 steps)](#1-how-it-works--end-to-end-flow)
2. [Design proposal](#2-design-proposal)
3. [Code changes — read this to understand the code](#3-code-changes)
4. [Standalone scripts to verify the full scan + query result](#4-verify-it-yourself)
5. [Status, limitations, and what's next](#5-status--limitations)

---

## 1. How it works — end‑to‑end flow

### Big picture

```
PRODUCER                 PULSAR BROKER                                  BOOKIE (BookKeeper)
--------                 -------------                                  -------------------
send(Person,             StreamLakeBatcher                             WriteEntryProcessorV3
  props=deptId,salary,     • buffer msgs (X ms / 2 MB / N)               • store entry (the page)
  eventTime)               • column-major encode  ──── addEntry ─────►   • recordPageRanges:
                           • per-column min/max     (data + ranges)          ledgerId+entryId =>
                           • date range                                       {deptId[min,max],
                           • ship ranges to bookie                             salary[min,max]}
                                                                              into RocksDB page-index
CONSUMER (pub/sub)       StreamLakeTranscoder
--------                   • read page entry  ◄──────── readEntry ──────  (the page bytes)
receive(Person)  ◄──────   • decode -> standard batch
                           • normal dispatch

CONSUMER (query)         StreamLakePageScan                            PagePruneProcessorV3
--------                   1. date-partition prune (DateIndex)           • giveIndexPages:
predicate(date,deptId,     2. bookie PAGE_PRUNE  ──── pagePrune ──────►     iterate ledger-page-index,
  salary)                     (push predicate)      (predicate ranges)      compare ranges, return
results  ◄───────────────  3. read only kept pages ◄─────────────────       matching entryIds
                           4. selective row decode
```

The numbered steps below are the canonical specification. Each maps to a concrete class/method.

---

**1. Batch data at the broker (X ms or max 2 MB per topic).**
For a topic flagged StreamLake, the broker buffers published messages per topic and seals a
**page** when it reaches the target size (`pageSizeBytes`, default 2 MB), message count
(`maxPageMessages`), **or** the grouping window (`pageGroupingDelayMs`, default a few ms) elapses.

> Code: `StreamLakeBatcher.add()` / `sealLocked()`
> (`pulsar-broker/.../service/streaminglake/StreamLakeBatcher.java`). The window is enforced by a
> scheduled `flush()` so a partial page still seals.

**2. Pack N entries into one container entry + column ranges + date range.**
On seal the broker derives, **from the topic's configured indexing fields**, the per‑column
min/max (e.g. `departmentId min=1,max=10`) and the page's `[minEventTime, maxEventTime]`. The
indexing fields are declared on the topic via `StreamingLakeConfig.indexedColumns`.

> Code: `StreamLakeRangeBuilder.buildForBatch()` (per‑column min/max as an opaque,
> order‑preserving blob) and `StreamLakeRangeBuilder.dateRange()` (event‑time range).
> The blob format is `PageRangeCodec` (bookie side, see step 8).

**3. Serialize the entries column‑major.**
The page stores each indexed column contiguously, e.g. for `Person{name, deptId, salary}`:

```
rows:    p1,p2,p3
deptId:  1, 1, 2     <- one contiguous column
salary:  10,25,30    <- one contiguous column
page-level ranges: deptId[min=1,max=2], salary[min=10,max=30]
```

> Code: `StreamLakeBatchPage.encode()` lays out a column directory + contiguous column arrays +
> the original message payloads. **Vortex is not wired** (no usable JVM binding); the JVM
> column‑major codec sits behind the same API and a `FLAG_VORTEX` bit is reserved for it.

**4. Version + encoding flag in the entry.**
The page header carries `magic='SLB2'`, a `version` byte, and a `flags` byte (`FLAG_COLUMNAR`,
and a reserved `FLAG_VORTEX`).

> Code: `StreamLakeBatchPage` header (`MAGIC`, `VERSION`, `FLAG_COLUMNAR`, `FLAG_VORTEX`).

**5. The broker sends a `byte[]` to the bookie, with two parts:**
 - **(a) column offsets inside the `byte[]`.** The page is column‑based, so it carries a
   **column directory** — `columnId, type, dataOffset` per column, plus a payload‑offset index —
   giving random access to any single column or any single row's payload.
 - **(b) column‑range metadata travels *alongside* the `byte[]`** (not inside it) so the bookie
   can index it separately.

> Code (a): `StreamLakeBatchPage` directory + `readColumn()` / `messageAt()`.
> Code (b): the ranges blob is passed as a **separate `addEntry` argument**, threaded
> `ManagedLedger.asyncAddEntry(buf, n, pageRanges, cb, ctx)` → `OpAddEntry` →
> `LedgerHandle.asyncAddEntry(data, pageRanges, …)` → V3 `AddRequest.pageRanges`.

**6. Pub/sub read decodes the entry back into a `List<entry>`.**
For a normal consumer, the broker reads the page entry and **transcodes** it back into a standard
Pulsar batch entry (re‑framing the stored messages under one `MessageMetadata` with
`numMessagesInBatch=N`), so the consumer client splits it natively — acks, keys, properties,
batch indices all work unchanged.

> Code: `StreamLakeTranscoder.transcodeInPlace()`, invoked at the top of
> `PersistentDispatcherMultipleConsumers.readEntriesComplete()`.

**7. Pulsar manages a date‑range ledger for date partitioning.**
Per‑ledger `[minDate, maxDate]` lives in a **single append‑only "date‑partition‑list" BookKeeper
ledger** per topic (entries `{dataLedgerId, minDate, maxDate}`, e.g. `2026-01-01 -> L1`). Its id
is stored in the managed‑ledger properties. On topic load the broker replays it to rebuild the
in‑memory view and rotates it into a fresh ledger (self‑healing if fenced).

> Code: `StreamLakeDateIndex` (`open()`, `record()`); pointer stored via `ManagedLedger.setProperty`.

**8. The bookie parses the column metadata separately and stores a page‑index.**
On write, the bookie persists the entry (the page) and **immediately** records its column ranges
into a RocksDB **page‑index** keyed by `ledgerId+entryId`:

```
key: L1-E1  =>  value: deptId[min=1,max=2], salary[min=10,max=30]
```

The bookie treats the ranges as **opaque, order‑preserving bytes** — it never decodes the page or
understands the schema. This is what keeps the bookie schema‑agnostic.

> Code: `WriteEntryProcessorV3` calls `recordPageRanges(...)` → `DbLedgerStorage` →
> `SingleDirectoryDbLedgerStorage.recordPageRanges()` → `PageRangeIndex.addPageRanges()`
> (RocksDB `page-ranges` column family). Range encode/compare: `PageRangeCodec`.

**9. On query, the broker first prunes by date‑range ledgers.**
The query carries a date window; the broker skips whole ledgers whose `[minDate,maxDate]` is
outside it — **before** touching the bookie.

> Code: `StreamLakePageScan.scan(...)` reads `PersistentTopic.getStreamLakeDateIndex()` and skips
> non‑overlapping ledgers (`ledgersPrunedByDate`).

**10. The broker pushes the predicate to a new bookie API `giveIndexPages`.**
For each surviving ledger the broker calls `pagePrune(startEntryId, endEntryId,
columnRangesForPredicate)`. The bookie iterates its page‑index from `ledgerId+startEntryId` to
`ledgerId+endEntryId`, compares each entry's stored ranges against the predicate ranges, and
returns the matching `entryId`s.

> Code: client `BookieClient.pagePrune()` → `PagePruneProcessorV3` (bookie) →
> `SingleDirectoryDbLedgerStorage.giveIndexPages()` → `PageRangeIndex.giveIndexPages()` +
> `PageRangeCodec.pageCouldMatch()`. Wire command: `PAGE_PRUNE` in `BookkeeperProtocol.proto`.

**11. The broker now has the pruned list of candidate pages.**
Only those page entries are read from the bookie (`PersistentTopic.asyncReadEntry`).

**12. A page has X rows but only Y match — decode only the matching rows.**
The broker reads just the predicate's **column** out of the page (e.g. `deptId`), finds the
matching row indices, and **materializes only those rows' payloads** — non‑matching messages are
never parsed (selective decode; the role Vortex would play).

> Code: `StreamLakePageScan.scan()` inner loop — `StreamLakeBatchPage.readColumn()` to filter,
> then `StreamLakeBatchPage.messageAt()` only for surviving rows.

**13. Return the matched rows to the query consumer.**
The surviving row payloads are returned (in the demo they are deserialized back into `Person`).

---

## 2. Design proposal

### Goals
- Columnar, analytics‑friendly storage **without breaking** Pulsar pub/sub.
- Push predicate evaluation **down to the bookie** so the broker reads only candidate pages.
- Keep the **bookie schema‑agnostic** (it only compares opaque, order‑preserving byte ranges).

### Key decisions
- **Opt‑in per topic.** `StreamingLakeConfig{enabled, batchingEnabled, pageSizeBytes,
  maxPageMessages, pageGroupingDelayMs, indexedColumns[]}` on `TopicPolicies`. Non‑StreamLake
  topics are completely untouched.
- **Page = one managed‑ledger entry.** N messages are packed into a single entry, so the existing
  ledger/cursor/replication machinery still applies; the entry just happens to be column‑major.
- **Ranges ride beside the data**, not inside it. `addEntry` gains an optional opaque
  `pageRanges` blob that the bookie indexes — the entry payload stays the page bytes.
- **Three‑level pruning:** (1) broker date‑partition prune over ledgers, (2) bookie `PAGE_PRUNE`
  over column ranges, (3) broker selective row decode.
- **Bookie indexes, doesn't interpret.** `key = ledgerId+entryId`, `value = per‑column min/max`
  in a dedicated RocksDB column family; comparison is unsigned‑byte order‑preserving.
- **Read path for pub/sub = transcode.** A page entry is re‑framed into a standard Pulsar batch
  on read so ordinary consumers are oblivious to the columnar storage.
- **Durable date index.** One append‑only ledger per topic, pointer in managed‑ledger metadata,
  rebuilt by replay; ~one entry per day keeps it well under 1K entries for years of data.

### Page format (v2)

```
magic(4)='SLB2' version(1) flags(1) numMessages(4) numCols(2) minDate(8) maxDate(8)
column directory:  numCols x [ columnId(2) type(1) dataOffset(4) ]
payloadIndexOffset(4)
per column:        numMessages values (INT=4B, LONG=8B) at its dataOffset   <- column-major
payload index:     (numMessages+1) x offset(4)
payload bytes:     each message's original headersAndPayload                 <- faithful reconstruction
```

### Bookie page‑index entry

```
RocksDB column family "page-ranges":
  key   = ledgerId(8) + entryId(8)
  value = PageRangeCodec.encodePage({ colId -> Range(minBytes, maxBytes) })   (opaque to bookie)
```

---

## 3. Code changes

Read top‑to‑bottom; this is the write path then the read path.

### BookKeeper — bookie side (schema‑agnostic page index)
| File | What changed |
|---|---|
| `bookkeeper-server/.../bookie/storage/ldb/PageRangeCodec.java` | Order‑preserving encode/decode of per‑column ranges; `encodePage`, `encode` (predicate), `pageCouldMatch`, `Range.overlaps`. |
| `bookkeeper-server/.../bookie/storage/ldb/PageRangeIndex.java` | RocksDB `page-ranges` CF; `addPageRanges(ledgerId,entryId,blob)` and `giveIndexPages(ledgerId,start,end,predicate)`. |
| `bookkeeper-server/.../bookie/storage/ldb/SingleDirectoryDbLedgerStorage.java`, `DbLedgerStorage.java` | `recordPageRanges(...)` and `giveIndexPages(...)` wired into ledger storage. |
| `bookkeeper-server/.../proto/WriteEntryProcessorV3.java` | After persisting an entry, if `AddRequest.hasPageRanges()`, call `recordPageRanges(...)`. |
| `bookkeeper-server/.../proto/PagePruneProcessorV3.java`, `PagePruneCompletion.java` | New `PAGE_PRUNE` request handler / client completion. |
| `bookkeeper-proto/.../BookkeeperProtocol.proto` | `AddRequest.pageRanges` field; `PAGE_PRUNE` operation + request/response. |

### BookKeeper — client side (carry ranges + new RPC)
| File | What changed |
|---|---|
| `proto/BookieClient.java`, `BookieClientImpl.java`, `PerChannelBookieClient.java` | `addEntry(..., pageRanges)` (V3 sets `AddRequest.pageRanges`); new `pagePrune(...)` RPC. |
| `client/LedgerHandle.java` | `asyncAddEntry(ByteBuf data, byte[] pageRanges, cb, ctx)` overload. |
| `client/PendingAddOp.java` | Carries `pageRanges`, sends it via the new `addEntry` signature. |

### Pulsar — ManagedLedger (thread ranges from the write path)
| File | What changed |
|---|---|
| `managed-ledger/.../ManagedLedger.java` | `asyncAddEntry(buffer, numberOfMessages, pageRanges, cb, ctx)` (default drops ranges). |
| `managed-ledger/.../impl/ManagedLedgerImpl.java` | Override honoring `pageRanges`. |
| `managed-ledger/.../impl/OpAddEntry.java` | `pageRanges` field; passes it to `LedgerHandle.asyncAddEntry`. |

### Pulsar — broker (policy, publish, dispatch, scan)
| File | What changed |
|---|---|
| `pulsar-common/.../policies/data/StreamingLakeConfig.java` | Topic config: `enabled, batchingEnabled, pageSizeBytes, maxPageMessages, pageGroupingDelayMs, indexedColumns[]`. |
| `pulsar-common/.../policies/data/HierarchyTopicPolicies.java`, `pulsar-broker/.../service/AbstractTopic.java` | Promote the policy into runtime; `isStreamLakeEnabled()`, `isStreamLakeBatched()`, `getStreamingLakeConfig()`. |
| `streaminglake/StreamLakeBatchPage.java` | **(step 3,4,5a)** column‑major page codec + offsets + version/flag + `readColumn`/`messageAt`. |
| `streaminglake/StreamLakeRangeBuilder.java` | **(step 2)** per‑column min/max blob, `extractColumns`, `dateRange`. |
| `streaminglake/StreamLakeBatcher.java` | **(step 1,2)** buffer + seal + write page with ranges + deferred acks; records date ranges. |
| `pulsar-broker/.../service/persistent/PersistentTopic.java` | `asyncAddEntry` routes StreamLake topics to the batcher; `getStreamLakeDateIndex()`. |
| `streaminglake/StreamLakeTranscoder.java` + `PersistentDispatcherMultipleConsumers.java` | **(step 6)** transcode page → standard batch on read. |
| `streaminglake/StreamLakeDateIndex.java` | **(step 7)** durable append‑only date‑partition‑list ledger. |
| `streaminglake/StreamLakePageScan.java` | **(steps 9‑13)** date prune → bookie `PAGE_PRUNE` → selective row decode. |

### Tests (all on a real broker + real bookie unless noted)
`StreamLakeRealBookieTestBase` (real `PulsarService` + `LocalBookkeeperEnsemble`),
`StreamLakePolicyTest` (flag), `StreamLakeManagedLedgerRangesTest` (2a),
`StreamLakePublishRangesTest` (per‑entry ranges), `StreamLakeBatchedPublishTest` (3A),
`StreamLakeTranscodingConsumerTest` (3B), `StreamLakePageScanTest` (3C),
`StreamLakeDatePruneTest` (3E), `StreamLakeDateIndexDurabilityTest` (durable index),
`StreamingLakePagePruneIntegrationTest` (bookie‑side, BookKeeper module).

---

## 4. Verify it yourself

`StreamLakeClusterDemo` runs the whole pipeline on an **in‑process standalone cluster** — a real
Pulsar broker (`new PulsarService(...).start()`) against a real ZooKeeper + real BookKeeper bookie
(`LocalBookkeeperEnsemble`) — and is started/stopped automatically.

```bash
cd pulsar/pulsar            # repo with build.gradle.kts
./build-all.sh --bk-only   # one-time: install vendored BookKeeper to mavenLocal
./streamlake-demo.sh       # start cluster -> run demo -> stop cluster
#   output dir defaults to /tmp/streamlake-out  (override: ./streamlake-demo.sh /my/dir)
```

What it does: creates a StreamLake (columnar, batched) topic with a **Person** schema
(`name, departmentId, salary`), publishes **1000** records across **10 date partitions**, then:

| File | Contents |
|---|---|
| `<out>/all-persons.txt` | all 1000 Person records, drained by a **normal consumer** (decoded from columnar pages by the transcoding dispatcher) |
| `<out>/filtered-persons.txt` | the **predicate query** result: `date_partition in [day3,day6] AND departmentId>5 AND departmentId<15 AND salary>50000` |

Expected (validated against a brute‑force oracle in the test):

```
# pruning: ledgers scanned=1, ledgers pruned by date=0
# matches=140 (brute-force oracle=140)
Person{name=person-311, departmentId=12, salary=52000}
...
days present: 3 4 5 6     departmentId range: 6-14     min salary: 52000
```

Run the whole verified test suite instead:

```bash
./gradlew :pulsar-broker:test --tests "org.apache.pulsar.broker.service.streaminglake.*"
```

---

## 5. Status & limitations

**Implemented and verified end‑to‑end on a real broker + real bookie:** steps 1, 2, 3
(column‑major; not Vortex), 4, 5a, 5b, 6, 7 (durable), 8, 9, 10, 11, 12, 13.

**Open items:**
- **Vortex codec (3/4/12):** no usable JVM binding; the JVM column‑major codec is used and a
  `FLAG_VORTEX` bit is reserved. Selective decode is implemented; vectorized Vortex decode is the
  drop‑in.
- **Multi‑ledger scan bug:** a query that scans **≥2 data ledgers** currently returns rows only
  from the first scanned ledger (the bookie page‑index lookup comes back empty for later
  ledgers). The demo keeps all pages in one ledger and filters dates via an indexed `day` column;
  ledger‑level date pruning itself is covered (single kept ledger) by `StreamLakeDatePruneTest`.
  **This is the next thing to fix.**
- **Producer‑receipt MessageId:** messages packed into one page share `(ledgerId, entryId)` in the
  producer's send receipt, because `PublishContext.completed` has no batch‑index slot. The
  consumer side is fully correct (transcode assigns native batch indices); a unique producer
  receipt needs a wire‑protocol/client change.
- **Dispatcher coverage / transcode assumptions:** the transcode hook is wired for the default
  Shared dispatcher; the same one‑line hook applies to the other dispatcher types. Transcoding
  assumes uncompressed messages and no broker‑entry‑metadata interceptor.
- **Predicate query is broker‑internal:** `StreamLakePageScan` is a broker‑side call; exposing it
  over the client/admin protocol (a query/read API) is needed for an external client to issue
  predicate scans.
