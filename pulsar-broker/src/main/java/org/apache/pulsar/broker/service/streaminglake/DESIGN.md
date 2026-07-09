# StreamLake — Design & End‑to‑End Flow

StreamLake is a columnar analytical / query layer built on a Pulsar + BookKeeper fork. It turns a
normal Pulsar topic into a **fresh‑data table store** you can prune, scan, filter, top‑K and
inner‑join with SQL — while the pub/sub write path stays a dumb, low‑latency pipe.

The core idea of the redesign:

* the **client** does the heavy lifting (batch → Apache Arrow columnar encoding → per‑batch pruning
  stats),
* the **broker** is a *dumb pipe*: it persists the client’s payload as a normal entry and slices the
  trailing stats footer into a metadata ledger — it never parses Arrow,
* the **bookie** is *pure storage*: normal ledger entries, no predicate work, no special index,
* a separate **query tier** does hierarchical pruning (date → segment → page) + late materialization
  + off‑heap hash join + bounded top‑K, driven by an **Apache Calcite** SQL frontend.

Isolation is what makes it safe on a live pub/sub cluster: StreamLake metadata ledgers (page‑index,
segment, catalog) can be pinned to a **separate bookie affinity group** with high RF for read
scaling, and the query‑executor role can run on **separate brokers** — so analytical load never
touches the pub/sub hardware. Pulsar geo‑replication then gives the table store multi‑region reads
for free.

---

## 1. Components

| Layer | Component | Module | Role |
|---|---|---|---|
| Client (write) | `StreamLakeProducer` | pulsar-client | Batches rows, flushes columnar messages |
| Client (write) | `StreamLakeArrowBatchEncoder` | pulsar-client | Rows → Apache Arrow IPC bytes |
| Client (write) | `StreamLakeStatsBuilder` / `StreamLakeBatchStats` | pulsar-client | Per‑batch pruning stats footer |
| Client (write) | `StreamLakeBatchPayload` | pulsar-client | Wire framing `[arrow][footer][len][magic]` |
| Client (read) | `StreamLakeConsumer` / `StreamLakeArrowBatchDecoder` | pulsar-client | Columnar message → rows |
| Client (read) | `StreamLakeScanPredicate` | pulsar-client | Pushdown predicate (prune + exact row filter) |
| Client (read) | `StreamLakeHashJoin`, `StreamLakeTopK` | pulsar-client | Off‑heap inner join, bounded top‑K |
| Broker (write) | `PersistentTopic` (`asyncAddEntry` / `addComplete`) | pulsar-broker | Persist entry + slice footer off the hot path |
| Broker (meta) | `StreamLakePageIndex` | pulsar-broker | Per‑page footer ledger chain |
| Broker (meta) | `StreamLakeSegmentStore` | pulsar-broker | Per‑segment merged‑stats ledger chain |
| Broker (meta) | `StreamLakeCatalog` | pulsar-broker | Per‑data‑ledger state + event‑time bounds |
| Broker (meta) | `StreamLakeMetaStore` | pulsar-broker | `/streamlake/<topic>` znode: ledger pointers |
| Broker (build) | `StreamLakeSegmentBuilder` / `StreamLakeStatsMerger` | pulsar-broker/-client | Compact closed ledger’s pages → segments |
| Query tier | `StreamLakePruner` | pulsar-broker | date → segment → page pruning |
| Query tier | `StreamLakeSqlPlanner` (Apache Calcite) | pulsar-broker | SQL → predicate + projection + sort/limit |
| Query tier | `StreamLakeQueryExecutor` | pulsar-broker | scan / top‑K / inner‑join engine |

Everything is gated by the per‑topic policy `StreamingLakeConfig` (`enabled`,
`clientColumnarEnabled`, indexed `columns`, `setMaxCardinality`, `bloomFpp`, RF knobs, isolation
group, query/offload flags).

---

## 2. Write path

### 2.1 Producer batching (`StreamLakeProducer`)

`StreamLakeProducer` wraps a raw `Producer<byte[]>` (created with **Pulsar batching disabled +
message compression on**) and accumulates rows in memory. It flushes a batch when **any** trigger
fires:

* `maxRows` buffered, or
* estimated `maxBytes` buffered, or
* `maxDelayMs` elapsed since the oldest buffered row (a background flusher keeps low‑rate producers
  fresh).

**One flush = one columnar batch = one Pulsar message = one BookKeeper entry (one “page”).**

### 2.2 Columnar conversion — why Apache Arrow

On flush the buffered rows are encoded column‑major with **Apache Arrow** (`StreamLakeArrowBatchEncoder`
→ Arrow IPC bytes). Arrow is used because:

* it is a **columnar** in‑memory/IPC format — the natural shape for analytical scans and for reading
  only the projected columns (late materialization),
* it is **zero‑copy / language‑agnostic** — the same bytes can later be handed to a native engine
  (DataFusion/Velox via the Arrow C Data Interface) without re‑encoding,
* its **schema/field metadata** carries the column types, so the payload is self‑describing (the
  Parquet/ORC “typed columnar” idea, but streaming‑friendly per batch).

`StreamLakeType` fixes the logical→Arrow type mapping (`INT32/INT64/DOUBLE/BOOLEAN/STRING/BYTES`).

> JDK note: Arrow 19 + `arrow-memory-unsafe` is used (the netty allocator collides with Pulsar’s
> Netty; Arrow 11 breaks on JDK 25). The test JVM opens `java.base/java.nio`.

### 2.3 Pruning stats footer (`StreamLakeBatchStats`)

For each **indexed** column the client computes an order‑preserving **min/max**, plus **one** of:

* a **low‑cardinality exact set** (≤ `setMaxCardinality` distinct values) → equality/`IN` prune with
  **no false positives**, even when the value is inside min/max; or
* a **high‑cardinality bloom** filter (`bloomFpp`) once the set would exceed the cap.

This directly answers the “bloom storage cost” concern: small text columns (e.g. `name`) keep a
per‑page exact set/bloom; large high‑cardinality columns (e.g. `email`) fall back to a single coarse
bloom, capping metadata size.

Footer layout (binary, `StreamLakeBatchStats.encode()`), magic `SLS1`:

```
MAGIC 'SLS1' | version(1) | numCols(varint) |
  per column: columnIndex | type | minLen|min | maxLen|max | distinctCount |
              setLen|[value...]  (low‑card)   OR   bloomLen|bloom  (high‑card)
```

Min/max/set values use `StreamLakeOrderPreserving` encoding: the **unsigned lexicographic order of the
bytes equals the typed order of the values**, so the broker/segment/query tier prune by raw
`Arrays.compareUnsigned` — schema‑blind, no type decoding.

### 2.4 Wire framing (`StreamLakeBatchPayload`)

The Arrow batch and the footer are framed into a single message payload with a fixed trailer so the
broker can slice the footer off the **tail** without parsing Arrow, and the consumer can recover the
Arrow bytes:

```
[ arrow IPC bytes ][ stats footer ][ footerLength : int32 ][ MAGIC 'SLP1' ]
```

The producer sends this as one `byte[]` message. On the wire this is a completely normal Pulsar
publish — **no new pub/sub protocol command is involved.**

### 2.5 Broker handling — the dumb pipe (`PersistentTopic`)

`PersistentTopic.asyncAddEntry` for a StreamLake topic just persists the payload as a normal
managed‑ledger entry:

```java
if (isStreamLakeEnabled()) {
    getOrCreateStreamLakePageIndex();               // ensure the page index exists
    ledger.asyncAddEntry(headersAndPayload, numMsgs, this, publishContext);
    return;
}
```

After the entry is durably written, `addComplete` (already off the ack‑critical path) slices the
footer and appends it to the page index **asynchronously**:

```java
if (pageIndex != null && StreamLakeBatchPayload.hasFooter(entryData)) {
    byte[] footer = StreamLakeBatchPayload.statsFooter(entryData);   // tail slice, no Arrow parse
    long dataLedgerId = position.getLedgerId();
    long dataEntryId  = position.getEntryId();
    executor.execute(() -> pageIndex.appendFooter(dataLedgerId, dataEntryId, footer));
}
```

The broker **never parses Arrow, never computes ranges, never asks the bookie to do predicate work.**
Best‑effort: if a footer append is lost, a scan simply falls back to the message’s own footer.

### 2.6 Bookie storage

The bookie stores **plain ledger entries** — the data pages in the topic’s managed ledger, and the
metadata entries in the page‑index / segment / catalog ledgers. There is **no** special bookie index
and **no** bookie‑side predicate API. (The old `PAGE_STATS`/`PAGE_PRUNE` RPCs + RocksDB page‑range
index were removed — see §8.)

---

## 3. Metadata

### 3.1 The `/streamlake` znode (`StreamLakeMetaStore`)

Each StreamLake topic owns a metadata node **separate** from the managed‑ledger znode, so StreamLake
updates never contend with ledger‑rollover metadata writes:

```
/streamlake/<tenant>/<namespace>/persistent/<topic>
```

Its value is a compact binary record (current `VERSION = 4`) holding **pointers** to the metadata
ledger chains (not the data itself):

```
version(1) | flags(1) |
  datePartitionLedgerId(8)          # if FLAG_DATE
  numSeg(4) | segmentLedgerIds(8*n) # segment index ledger chain
  numPi(4)  | pageIndexLedgerIds(8*n)  # page‑index ledger chain
  catalogLedgerId(8)                # if FLAG_CATALOG
```

Example (decoded) znode content:

```jsonc
// /streamlake/acme/sales/persistent/orders
{
  "catalogLedgerId":     40001,          // per‑data‑ledger state + event‑time bounds
  "pageIndexLedgerIds": [50010, 50011],  // chain (head rolls at 4 MB / on fence)
  "segmentLedgerIds":   [60005],         // chain of merged‑stats segment ledgers
  "datePartitionLedgerId": 30002         // legacy/date bounds (optional)
}
```

Updates use optimistic concurrency against this node only (`store.put(path, bytes, expectedVersion)`).
`VERSION_V1..V3` remain **decode‑only** for forward migration of older records.

### 3.2 Catalog ledger (`StreamLakeCatalog`)

A single append‑only BookKeeper ledger (id in the znode) with one record per data ledger
(`ENTRY_SIZE = 8+8+8+8+8+1`):

```
dataLedgerId(8) | createTs(8) | minEventTime(8) | maxEventTime(8) | rowCount(8) | state(1)
state ∈ { OPEN, CLOSED, SEGMENTED }
```

* **read path** uses `[minEventTime, maxEventTime]` for coarse **date pruning**
  (`candidateLedgers(fromMs, toMs)`),
* **segment builder** uses `state` to find closed‑but‑not‑yet‑segmented ledgers
  (`closedUnsegmented()`).

Latest entry per data ledger wins on replay; the ledger self‑heals (replay → rewrite into a fresh
ledger) if the previous head was fenced.

### 3.3 Page‑index ledger chain (`StreamLakePageIndex`)

One **chain** of BookKeeper ledgers per topic (ids in the znode). Each entry is one page’s footer,
keyed by the data entry it describes:

```
ENTRY: 'F' | dataLedgerId(8) | dataEntryId(8) | <stats footer bytes>
```

The head ledger rolls when it reaches `maxHeadBytes` (default 4 MB) or is fenced, so a data ledger’s
footers may span a roll. Memory‑light: only lightweight references
`dataLedgerId -> [(piLedgerId, piEntryId, dataEntryId)]` are held in the broker; footer bodies stay
in the ledger and are read on demand by `footersFor(dataLedgerId)`. On `open()` the chain is replayed
to rebuild the references (footer bodies skipped).

### 3.4 Segment ledger chain (`StreamLakeSegmentStore`) — see §4.

---

## 4. Segment creation (async, after a data ledger closes)

Segments are the **coarse, hot pruning layer**: one segment summarizes a contiguous run of a data
ledger’s pages with **merged** per‑column stats, so a scan can skip whole entry ranges before ever
touching per‑page footers.

### 4.1 Trigger after ledger close

When a data ledger rolls **closed**, the broker records it `CLOSED` in the catalog. Segment building
is an **asynchronous, out‑of‑band consumer** (kept off the write path): the owning broker enqueues
the closed ledger (design: a message on an internal **system topic**, sharded by topic so builds run
in parallel across topics and ordered per topic). A **segment‑build consumer** (a query‑tier broker)
picks it up and runs `StreamLakeSegmentBuilder`.

> Implementation status: the builder and its work‑queue (`buildForLedger` / `buildAllClosed()` over
> `catalog.closedUnsegmented()`) are implemented and tested; wiring the system‑topic trigger consumer
> into broker startup is the remaining integration point (today the builder is invoked directly / in
> tests).

### 4.2 How the consumer builds a segment (`StreamLakeSegmentBuilder`)

`buildForLedger(dataLedgerId)` (idempotent — skips an already‑`SEGMENTED` ledger):

1. Read the page footers for the data ledger from `StreamLakePageIndex.footersFor(...)`
   (**metadata only — never the data pages**).
2. Group every `pagesPerSegment` consecutive pages.
3. Merge each group’s per‑column stats with `StreamLakeStatsMerger.merge(...)`:
   * union min/max always;
   * union low‑cardinality exact sets while they stay ≤ cap;
   * promote to a segment‑level bloom once the union would exceed the cap (or any input was already a
     bloom) — so a segment keeps exact sets for small columns and one coarse bloom for large ones;
   * **no false negatives** — a segment never excludes a page that could match.
4. Append one `Segment` per group to `StreamLakeSegmentStore`.
5. Mark the data ledger `SEGMENTED` in the catalog.

### 4.3 What is in a segment / the segment ledger

`StreamLakeSegmentStore` is a chain of BookKeeper ledgers (ids in the znode). Each entry:

```
ENTRY: 'S' | dataLedgerId(8) | startEntry(8) | endEntry(8) | statsLen(4) | <merged stats>
Segment = { dataLedgerId, startEntry, endEntry, StreamLakeBatchStats(merged) }
```

Unlike the page index, **segments are held in memory** (small, and the hot pruning layer); they are
replayed on `open()`, and a fenced/closed head rolls to a fresh ledger. `setSegmentLedgerIds(...)`
records the chain in the znode.

---

## 5. Read path

### 5.1 SQL frontend — Apache Calcite (`StreamLakeSqlPlanner`)

A SQL string is parsed with **Apache Calcite** (`SqlParser`) and translated against the topic’s
`StreamLakeSchema` into an executor‑ready plan:

* `SELECT <cols>|*` → **projection** (column indexes into the full schema),
* `FROM <topic>` → table,
* a conjunctive (`AND`‑only) `WHERE` of `=, >, >=, <, <=, BETWEEN, IN` (either operand order) →
  `StreamLakeScanPredicate` (strict `<`/`>` map to exclusive bounds so the exact row filter is
  correct, not just conservative),
* `ORDER BY <col> [ASC|DESC]` + `LIMIT k` → bounded **top‑K**,
* predicates on the configured **event‑time column** become the `[fromMs, toMs]` **date‑prune window**
  (and are dropped from the row filter — date pruning already handles them).

Column names resolve case‑insensitively. Calcite owns parsing; StreamLake owns pruning + execution.

### 5.2 Hierarchical pruning (`StreamLakePruner`)

`prune(fromMs, toMs, predicate)` narrows work top‑down, and is **conservative** (never a false
negative — survivors are still exactly row‑filtered on read):

1. **Date → ledger:** `catalog.candidateLedgers(fromMs, toMs)` keeps only data ledgers whose
   `[minEventTime, maxEventTime]` intersects the window.
2. **Segment → range:** for each candidate ledger, drop whole segments whose **merged** stats can’t
   match the predicate (`predicate.matches(segment.stats)`), yielding surviving `[startEntry,endEntry]`
   ranges. If a ledger isn’t segmented yet, fall back to a full per‑page prune of that ledger.
3. **Page → entry:** within surviving ranges, keep only pages whose **per‑page footer** can match.
   Result: a list of `PagePointer(dataLedgerId, entryId)` to read.

Pruning reads **only metadata** (catalog + segment store in memory, page footers on demand) — never
the 1 MB data pages, and never asks the bookie to evaluate anything.

### 5.3 Scan + late materialization (`StreamLakeQueryExecutor`)

`scan(fromMs, toMs, predicate)`:

1. `pages = pruner.prune(...)`.
2. For each surviving page, `PageReader.readArrowBatch(ledgerId, entryId)` returns the raw Arrow bytes
   (in a broker it reads the managed‑ledger entry and strips the `StreamLakeBatchPayload` framing back
   to the Arrow IPC), and `StreamLakeArrowBatchDecoder` materializes rows.
3. Each row is **exactly** re‑checked with `predicate.matchesRow(row)` (pruning was conservative).

`executeSql(sql, schema, timeColumn)` composes it end to end: parse → prune → read → exact filter →
`ORDER BY/LIMIT` (bounded top‑K) → projection.

### 5.4 Consumer columnar deserialize (`StreamLakeConsumer`)

For pub/sub delivery of a StreamLake topic, `StreamLakeConsumer` wraps a raw `Consumer<byte[]>`:
`StreamLakeBatchPayload.arrowBatch(payload)` recovers the Arrow bytes and
`StreamLakeArrowBatchDecoder` yields the original rows — so a normal subscriber transparently reads
back the columnar batch as records. (This replaced the old broker‑side read transcoder.)

### 5.5 Full scan — worked example

`SELECT id, salary FROM employee WHERE deptId = 1 AND salary >= 300000 ORDER BY salary DESC LIMIT 10`

1. Calcite → predicate `{deptId = 1, salary ≥ 300000}`, projection `[id, salary]`, sort `salary DESC`,
   limit `10`, window `[MIN, MAX]` (no time predicate).
2. Date prune → candidate data ledgers (all, if unbounded window).
3. Segment prune → skip segments whose merged `deptId` set doesn’t contain `1` **or** whose `salary`
   max `< 300000`.
4. Page prune → within surviving segments, keep pages whose footer `deptId` set contains `1` and
   `salary` max `≥ 300000`.
5. Read surviving pages’ Arrow, decode rows, keep rows where `deptId == 1 && salary >= 300000`.
6. Bounded top‑K (`k = 10`, `salary DESC`) over the survivors; project to `[id, salary]`.

---

## 6. Inner join — end to end

`StreamLakeHashJoin` is a classic build/probe hash join sized for the **smaller, already‑pruned**
side. `StreamLakeQueryExecutor.scanInnerJoin(...)` wires two pruned scans together and emits
`concat(probeRow, buildRow)` for matching keys.

Example: **orders ⋈ customers on customerId**, “VIP customers’ recent big orders”:

```
build side : SELECT customerId, name FROM customers WHERE tier = 'VIP'
probe side : SELECT orderId, customerId, amount FROM orders
             WHERE ts >= <last 24h> AND amount >= 500
join key   : customerId
```

Steps:

1. **Plan** both sides with Calcite → two `StreamLakeScanPredicate`s (+ the `orders` time predicate
   becomes the probe’s date window).
2. **Prune + scan the build side** (`customers WHERE tier = 'VIP'`): date → segment → page prune, read
   surviving pages, exact‑filter `tier == 'VIP'`. This side is the smaller, so it becomes the hash
   table: `join.addBuildRow(row)` keying on `customerId` (bounded by `maxBuildRows`, off‑heap
   friendly).
3. **Prune + scan the probe side** (`orders …`): the `ts` window restricts candidate data ledgers by
   event time (cheap date prune), then segment/page prune on `amount >= 500`, read + exact‑filter.
4. **Probe:** for each order row, look up `customerId` in the hash table; on a hit emit
   `concat(order, customer)` = `[orderId, customerId, amount, customerId, name]`.
5. Optionally feed the joined stream into `StreamLakeTopK` (e.g. top orders by `amount`) and project.

Because both sides are pruned to *fresh* candidate pages first, the join only materializes the rows
that survive pruning + row filter — the build side stays small and the probe side streams.

---

## 7. Why it stays safe next to pub/sub

* **Metadata isolation:** page‑index / segment / catalog ledgers can be pinned to a separate bookie
  affinity group (`metadataBookieAffinityGroup`) with high RF (e.g. RF‑50) so analytical reads scale
  horizontally without loading the pub/sub bookies.
* **Compute isolation:** the query‑executor + segment‑build roles run on separate brokers
  (`queryExecutorEnabled`) with their own hardware profile.
* **Durability:** everything is a durable BookKeeper ledger; nothing critical lives only in broker
  memory. Cold segments can be offloaded to object storage (`segmentOffloadEnabled`).
* **Geo‑replication:** Pulsar’s built‑in geo‑replication carries the topic (and thus the table) across
  regions.

---

## 8. Change log — what was built / removed

**Client (pulsar-client) — new columnar write/read + operators**
* `StreamLakeProducer` (row batching + flush triggers), `StreamLakeArrowBatchEncoder/Decoder`
  (Apache Arrow), `StreamLakeSchema` / `StreamLakeType`, `StreamLakeStatsBuilder` /
  `StreamLakeBatchStats` / `StreamLakeBloom` / `StreamLakeOrderPreserving` (pruning stats footer),
  `StreamLakeBatchPayload` (wire framing), `StreamLakeConsumer` (columnar deserialize),
  `StreamLakeScanPredicate` (pushdown + strict bounds), `StreamLakeStatsMerger`,
  `StreamLakeHashJoin`, `StreamLakeTopK`.

**Broker (pulsar-broker) — dumb‑pipe write + metadata + query tier**
* `PersistentTopic`: persist StreamLake payload as a normal entry and slice the stats footer into the
  page index in `addComplete` (off the hot path).
* Metadata: `StreamLakeMetaStore` (`/streamlake/<topic>` znode, v4), `StreamLakeCatalog`
  (per‑data‑ledger state + event‑time bounds), `StreamLakePageIndex` (per‑page footer ledger chain),
  `StreamLakeSegmentStore` (segment ledger chain).
* Build: `StreamLakeSegmentBuilder` (compact closed ledger’s pages → segments).
* Query: `StreamLakePruner` (date→segment→page), `StreamLakeSqlPlanner` (Apache Calcite SQL frontend),
  `StreamLakeQueryExecutor` (scan / top‑K / inner‑join).
* Config: `StreamingLakeConfig` policy (enable, indexed columns, cardinality/bloom, RF + isolation,
  query/offload flags).

**Removed dead code (phase G)**
* Broker prototype path: `StreamLakeBatcher`, `StreamLakeBatchPage`, `StreamLakeColumnCodec`,
  `StreamLakeRangeBuilder`, `StreamLakeTranscoder`, `StreamLakePageScan`, `StreamLakeStore`,
  `StreamLakeJoin`, `StreamLakeDateIndex`, `StreamingLakeScanExecutor/Predicate/ScanService`,
  `SegmentSummary(Codec)`, `ColumnarPage`, `DateIndexLedger` + wiring in `PersistentTopic` /
  `PersistentDispatcherMultipleConsumers` / `AbstractTopic`.
* Bookie “range API”: the `PAGE_STATS` / `PAGE_PRUNE` RPCs, `PageStats/PagePruneProcessorV3` +
  completions, the `LedgerStorage.recordPageRanges/giveIndexPages/scanPageStats` methods, the RocksDB
  `PageRangeIndex` / `PageStatEntry` / `PageRangeCodec`, the `AddRequest.pageRanges` field and the
  whole `addEntry(pageRanges)` overload chain (`LedgerHandle` / `PendingAddOp` / `BookieClient(Impl)` /
  `PerChannelBookieClient`), plus the `ManagedLedger.asyncAddEntry(pageRanges)` overload.
* Client↔broker scan RPC: `CommandScan` / `CommandScanResponse` (proto), `Type.SCAN/SCAN_RESPONSE`,
  and the `Commands` / `PulsarDecoder` / `ServerCnx` / `ClientCnx` handlers.
* Old `StreamingLakeConfig` knobs that only drove the prototype (batching/page/granule/compression).
