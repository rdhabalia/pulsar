# StreamLake — Detailed Design & Implementation

StreamLake turns a Pulsar topic into a **fresh‑data columnar table store** you can prune, scan,
filter, top‑K and inner‑join with SQL, while the pub/sub write path stays a dumb, low‑latency pipe.

**Division of labour**

* **Client** does the heavy work: batch rows → **Apache Arrow** columnar encoding → per‑batch pruning
  stats (min/max + exact‑set/bloom) → frame into one message.
* **Broker** is a *dumb pipe*: persist the client’s payload as a normal managed‑ledger entry, then
  slice the trailing stats footer and append it to a **page‑index ledger** — a **durability barrier**
  before the producer ack (a successful publish guarantees the page is prunable). It never parses
  Arrow, never computes ranges, never asks the bookie to evaluate predicates.
* **Bookie** is *pure storage*: normal ledger entries only. No RocksDB page index, no predicate RPC.
* **Query tier** does hierarchical pruning (date → segment → page) + late materialization + off‑heap
  hash join + bounded top‑K, driven by an **Apache Calcite** SQL frontend.

### How this differs from the original brainstorm (what actually got built)

The earlier design had the broker batch/encode and the **bookie** hold a RocksDB page index that the
segment builder later read via a `PAGE_STATS`/`PAGE_PRUNE` RPC and then *deleted*. That path was
dropped. In the shipped design:

* the **client** batches/encodes (broker/bookie do no columnar work);
* page stats live in a **dedicated page‑index BookKeeper ledger chain** written by the broker — never
  in the bookie’s RocksDB, so there is **no copy‑then‑delete** and **no bookie range API**;
* a **segment** is **column‑oriented**: per data ledger, one entry per indexed column holding that
  column's **per‑page array** (min/max, plus a per‑page bloom for text), collapsing to one coarse stat
  only past a size cap — so pruning lands on the exact page from the segment alone;
* SQL is parsed by **Apache Calcite**; `ORDER BY … LIMIT` is a bounded top‑K heap.

---

## 0. Registration

An admin marks a topic StreamLake and registers the ordered schema + which columns are indexed, via
the topic policy `StreamingLakeConfig` (a plain policy POJO, no proto):

```
StreamingLakeConfig {
  enabled = true
  clientColumnarEnabled = true
  columns = [ {id:0 INT32}, {name:1 STRING idx}, {deptId:2 INT32 idx},
              {email:3 STRING idx}, {salary:4 INT64 idx} ]   // ordered; index = position
  setMaxCardinality = 64        // exact-set cap per column, else bloom
  bloomFpp = 0.01
  pagesPerSegment = 1024
  pageIndex{Ensemble,WriteQuorum,AckQuorum} = 3/3/2     // hot metadata RF
  segment{Ensemble,WriteQuorum,AckQuorum}   = 5/5/3     // read-scalable RF
  metadataBookieAffinityGroup = "sl-meta"  // isolate metadata ledgers off the pub/sub bookies
  queryExecutorEnabled, segmentOffloadEnabled
}
```

`StreamLakeTopicSchema.fromConfig(cfg)` derives the client encode/stats view: the `StreamLakeSchema`
(name→`StreamLakeType`), the indexed‑column index list, and the cardinality/bloom knobs. The
column’s **index = its position in `columns`**; the footer and segment reference columns by that int.

Types (`StreamLakeType` → Apache Arrow): `INT32→Int(32)`, `INT64→Int(64)`, `DOUBLE→FloatingPoint`,
`BOOLEAN→Bool`, `STRING→Utf8`, `BYTES→Binary`.

---

## 1. WRITE PATH (step by step)

### 1.1 Producer batching — `StreamLakeProducer`

Wraps a raw `Producer<byte[]>` (create it with **Pulsar batching disabled + message compression on**).
`addRow(Object[])` (row aligned to schema columns; nulls allowed):

```java
if (buffer.isEmpty()) oldestRowNanos = nanoTime();   // start the delay clock
buffer.add(row);
bufferedBytes += estimate(row);                       // cheap size heuristic
if (buffer.size() >= maxRows || bufferedBytes >= maxBytes) return doFlush();
```

Flush triggers (defaults `maxRows=1000`, `maxBytes=1 MiB`, `maxDelayMs=10`): row count, estimated
bytes, **or** a background `streamlake-flush` thread that fires `doFlush()` once the oldest buffered
row is `≥ maxDelayMs` old (keeps low‑rate producers fresh). **One flush = one columnar batch = one
Pulsar message = one BookKeeper entry = one “page”.**

`doFlush()` (the whole write encode in 4 lines):

```java
byte[] arrow  = encoder.encode(rows);                                  // (1.2) Arrow IPC
byte[] footer = StreamLakeStatsBuilder.build(schema, rows,
                   indexedColumns, setMaxCardinality, bloomFpp).encode(); // (1.3/1.4) stats footer
byte[] payload = StreamLakeBatchPayload.combine(arrow, footer);        // (1.5) framing
producer.sendAsync(payload);                                            // (1.6) normal publish
```

### 1.2 Columnar encoding — `StreamLakeArrowBatchEncoder` (why Arrow)

Builds a `VectorSchemaRoot` from `schema.toArrowSchema()`, fills one typed **column vector** per field
(`IntVector`/`BigIntVector`/`Float8Vector`/`BitVector`/`VarCharVector`/`VarBinaryVector`, nulls via
`setNull`), sets the row count, and writes an **Arrow IPC stream** (`ArrowStreamWriter`:
`start(); writeBatch(); end()`) to a byte array. Uncompressed at the IPC layer — Pulsar message
compression handles bytes.

Arrow is used because it is **columnar** (read only projected columns → late materialization),
**self‑describing** (the IPC stream carries its own schema, so the decoder needs no external schema),
and **zero‑copy / language‑agnostic** (the same bytes can later feed a native engine — DataFusion/Velox
— over the Arrow C Data Interface without re‑encoding). Runtime: Arrow 19 + `arrow-memory-unsafe`
(the netty allocator collides with Pulsar’s Netty; Arrow 11 breaks on JDK 25); the test JVM opens
`java.base/java.nio`.

### 1.3 Pruning stats — `StreamLakeStatsBuilder` + order‑preserving encoding

For each **indexed** column, over the batch’s rows:

```java
TreeSet<byte[]> distinct = new TreeSet<>(Arrays::compareUnsigned);   // sorted, dedup
for (row) if (v != null) distinct.add(StreamLakeOrderPreserving.encode(type, v));
min = distinct.first(); max = distinct.last();
if (distinct.size() <= setMaxCardinality)  ColumnStats(min,max, exactSet=distinct)   // low-card
else                                        ColumnStats(min,max, bloom=Bloom(distinct,fpp)) // high-card
```

**Order‑preserving encoding** (`StreamLakeOrderPreserving`) is the crux: the *unsigned lexicographic
order of the encoded bytes equals the typed order of the values*, so every downstream prune is a raw
`Arrays.compareUnsigned` — schema‑blind, no type decoding:

* INT32/INT64: big‑endian with the **sign bit flipped** (`v ^ 0x80…0`) so negatives sort before
  positives as unsigned bytes.
* DOUBLE: IEEE‑754 bits, then `bits ^= (bits>>63) | 0x80…0` (flip sign for positives, all bits for
  negatives).
* BOOLEAN: `{0}`/`{1}`. STRING: UTF‑8 bytes. BYTES: raw.

This is exactly the *“MIN/MAX for numbers, set(N) for text”* idea, generalized: small columns keep a
per‑page **exact set** (equality/`IN` prune with **no false positives**, even inside min/max); large
high‑cardinality columns (e.g. `email`) fall back to **one bloom per page**, capping metadata size.

### 1.4 Stats‑footer byte layout — `StreamLakeBatchStats.encode()`

```
'SLS1'(4) | version(1) | numCols : int32 |
  repeat numCols:
    columnIndex : int32
    typeOrdinal : int8                 // StreamLakeType.ordinal()
    flags       : int8                 // bit0 = has min/max, bit1 = has exactSet, bit2 = has bloom
    distinctCount : int32              // -1 at a segment => high-card (min/max only)
    if bit0:  minLen:int32|min[]   maxLen:int32|max[]
    if bit1:  setCount:int32   then setCount × (len:int32|value[])   // sorted, order-preserving
    if bit2:  bloomLen:int32|bloom[]
```

`decode()` reverses it. `ColumnStats` exposes the two prune primitives used everywhere:

* `overlaps(lo,hi)` → `compareUnsigned(lo,max) ≤ 0 && compareUnsigned(hi,min) ≥ 0` (range skip).
* `mightContain(encodedValue)` → `binarySearch(exactSet)` (exact) **or** `bloom.mightContain` (fpp)
  **or** `true` if no membership filter (min/max only).

### 1.5 Wire framing — `StreamLakeBatchPayload.combine()`

The footer goes on the **tail** so the broker can slice it without touching Arrow:

```
[ arrow IPC bytes ][ stats footer ][ footerLength : int32 ][ MAGIC 'SLP1'(4) ]
```

* `hasFooter(payload)` → check trailing magic.
* `statsFooter(payload)` → read `footerLength`, copy `[len-8-footerLength, len-8)`.
* `arrowBatch(payload)` → `[0, len-8-footerLength)`.

### 1.6 Send + 1.7 Broker dumb pipe — `PersistentTopic`

The producer sends `payload` as an ordinary `byte[]` message — **no new pub/sub protocol command**.
On the broker, `asyncAddEntry` for a StreamLake topic just persists it as a normal entry:

```java
if (isStreamLakeEnabled()) {
    getOrCreateStreamLakePageIndex();                          // lazily open the page-index ledger
    ledger.asyncAddEntry(headersAndPayload, numMsgs, this, publishContext);
    return;
}
```

After the data entry is durable, `addComplete` slices the footer and appends it to the page‑index
ledger as a **durability barrier — the producer is acked only after the page‑index write succeeds**
(the pruning metadata is guaranteed present, so a successful publish is always queryable/prunable).
The append runs on a **per‑topic ordered executor** (appends stay in data‑entry order — the segment
builder groups consecutive pages — and dedup stays monotonic), off the managed‑ledger callback thread:

```java
if (pageIndex != null && StreamLakeBatchPayload.hasFooter(entryData)) {
    byte[] footer = StreamLakeBatchPayload.statsFooter(entryData);   // tail slice, no Arrow parse
    long   lid    = position.getLedgerId();
    long   eid    = position.getEntryId();
    brokerService.getTopicOrderedExecutor().executeOrdered(topic, () -> {
        try {
            pageIndex.appendFooter(lid, eid, footer);                // (1.8) durable, honors ack quorum
            messageDeduplication.recordMessagePersisted(publishContext, position);
            publishContext.completed(null, lid, eid);                // ack AFTER the page-index write
        } catch (Exception e) {
            publishContext.completed(new PersistenceException(e), -1, -1);  // fail (retriable)
        } finally {
            decrementPendingWriteOpsAndCheck();
        }
    });
    return;
}
```

On failure the publish is **failed (retriable)**, not acked, and dedup is *not* recorded — so a
producer retry is reprocessed (run StreamLake topics with **producer dedup enabled** so the retry
does not duplicate the already‑persisted data entry). The message still embeds its own footer, so the
page‑index is additionally rebuildable by a background reconciler as a belt‑and‑suspenders.

### 1.8 Page‑index ledger append — `StreamLakePageIndex.appendFooter()`

```java
byte[] entry = encode(dataLedgerId, dataEntryId, footer);   // 'F'|dataLid(8)|dataEid(8)|footer
ensureHeadFor(entry.length);                                 // roll head if > maxHeadBytes (4 MiB)
long piEntryId = addToHead(entry);                           // append to head BK ledger (see below)
refsByDataLedger.get(dataLedgerId).add(new Ref(head.getId(), piEntryId, dataEntryId));  // in-mem
```

`addToHead` does a synchronous `head.addEntry(entry)` — a BK write that returns once **`ackQuorum`**
bookies ack (writing to all **`ensembleSize`**); a fenced/closed head is transparently rolled and
retried once. The head ledger is created with the topic’s configured replication:

```java
bk.createLedger(ensembleSize, writeQuorum, ackQuorum, CRC32, PASSWORD)
```

from `StreamingLakeConfig.pageIndex{EnsembleSize,WriteQuorum,AckQuorum}` (default **3/3/2**). A **high
ensemble** spreads this hot pruning metadata across many bookies for read scaling; a **smaller ack
quorum** (write to many, wait for a few) keeps publish latency low. Segment ledgers use the separate
`segment{…}Quorum` (default 5/5/3, tunable much higher).

**Memory‑light:** only lightweight references `dataLedgerId → [(piLedgerId, piEntryId, dataEntryId)]`
live in the broker; the footer *bodies* stay in the ledger and are read on demand by `footersFor()`.
This is what keeps broker memory bounded even at 10K+ pages/ledger (the concern from the brainstorm).

### 1.9 Bookie

Stores **plain ledger entries** — the topic data pages and the metadata ledgers (page‑index, segment,
catalog). No special index, no predicate work.

### 1.10 Catalog upsert — `StreamLakeCatalog`

On rollover / close the broker records the data ledger’s `{createTs, minEventTime, maxEventTime,
rowCount, state}` (`upsert` / `markState`). `minEventTime/maxEventTime` drive **date pruning**;
`state` (`OPEN→CLOSED→SEGMENTED`) drives the segment‑build queue.

---

## 2. METADATA (exact formats)

### 2.1 The `/streamlake` znode — `StreamLakeMetaStore`

A node **separate** from the managed‑ledger znode (so StreamLake writes never contend with
ledger‑rollover metadata), path:

```
/streamlake/<tenant>/<namespace>/persistent/<topic>
```

Value = compact binary (`encode()`), current `VERSION=4`:

```
version:int8 | flags:int8 |
datePartitionLedgerId:int64 |            // 0 if unset; present when flags & FLAG_DATE(0x1)
nSeg:int32   | segmentLedgerId  × nSeg (int64 each) |
nPi:int32    | pageIndexLedgerId × nPi (int64 each) |
catalogLedgerId:int64                    // present when flags & FLAG_CATALOG(0x4)
```

Only **ledger‑id pointers** live here (never the stats). Updates use optimistic concurrency against
this node only: `store.put(path, encode(rec), Optional.of(expectedVersion))`, retried on
`BadVersion`. `VERSION_V1..V3` remain **decode‑only** for forward migration.

Example (decoded):

```jsonc
// /streamlake/acme/sales/persistent/orders   (VERSION=4, flags=DATE|CATALOG)
{
  "datePartitionLedgerId": 30002,
  "segmentLedgerIds":      [60005],          // segment ledger chain
  "pageIndexLedgerIds":    [50010, 50011],   // page-index ledger chain (head rolled once)
  "catalogLedgerId":       40001             // per-data-ledger state + event-time bounds
}
```

### 2.2 Catalog ledger — `StreamLakeCatalog`

One append‑only BK ledger (id in the znode). Fixed‑size entry (`ENTRY_SIZE = 8+8+8+8+8+1 = 41`):

```
dataLedgerId:int64 | createTs:int64 | minEventTime:int64 | maxEventTime:int64 | rowCount:int64 | state:int8
state: 0=OPEN 1=CLOSED 2=SEGMENTED
```

`open()` → `loadAndRotate()`: open the old ledger, replay entries (**latest wins** per
`dataLedgerId`), write survivors into a **fresh** ledger, point the znode at it, delete the old
(self‑heals a fenced ledger). Reads: `candidateLedgers(fromMs,toMs)` = ledgers whose `[minEt,maxEt]`
intersects the window; `closedUnsegmented()` = the segment‑build queue.

### 2.3 Page‑index ledger chain — `StreamLakePageIndex`

A **chain** of BK ledgers per topic (ids in the znode). Entry:

```
'F'(1) | dataLedgerId:int64 | dataEntryId:int64 | <stats-footer bytes>
```

Head rolls at `maxHeadBytes` (default 4 MiB) or on fence, so a data ledger’s footers may span a roll.
`open()` replays the chain (`bk.openLedger` → read `[0..LAC]` → parse header, **skip the footer
body**) to rebuild the in‑memory `refsByDataLedger`. `footersFor(dataLedgerId)` reads each referenced
entry on demand and returns `[(dataEntryId, footerBytes)]`. Production RF is `pageIndex{…}Quorum`
(default 3/3/2) on the isolated metadata pool.

### 2.4 Segment ledger chain — `StreamLakeSegmentStore`

A chain of BK ledgers (ids in the znode). A data ledger's segment is **column‑oriented**: one page
**directory** entry (position → data entryId) followed by one **column** entry per indexed column,
each carrying that column's per‑page stats:

```
'D'(1) | dataLedgerId:int64 | numPages:int32 | pageEntryId:int64 x numPages
'C'(1) | dataLedgerId:int64 | blobLen:int32  | StreamLakeColumnSegment.encode()
```

`StreamLakeColumnSegment.encode()`:

```
columnIndex:int32 | type:int8 | collapsed:int8 | numPages:int32 |
  if !collapsed: per page:  flags:int8 [minLen|min maxLen|max]? [bloomLen|bloom]?
  if  collapsed: one stat:  flags:int8 [minLen|min maxLen|max]? [bloomLen|bloom]?
```

So a column keeps a **per‑page array** — numeric columns store `[min,max]` per page; text/bytes
columns also store a per‑page **bloom** — until the array would exceed `segmentColumnMaxBytes`
(default 2 MiB), at which point the column **collapses** to one whole‑segment stat. Segments are held
in memory (`byLedger: dataLedgerId → LedgerSegment{pageEntryIds[], columns}`), replayed fully on
`open()`. RF is `segment{…}Quorum` (default 5/5/3, tunable much higher for read scaling).

---

## 3. ASYNC SEGMENT BUILD (after a data ledger closes)

A **segment** is the column‑oriented, per‑page index for a data ledger: a scan prunes straight to the
exact candidate pages from it, so once a ledger is segmented pruning no longer reads its per‑page
footers at all.

### 3.1 Trigger

When a data ledger rolls **closed**, the broker marks it `CLOSED` in the catalog. Building is
**out‑of‑band** (off the write path): the owning broker enqueues the closed ledger — design: publish a
`{topic}` message onto an internal **sharded system topic** (shard by topic → parallel across topics,
ordered per topic). A **segment‑build consumer** (a query‑tier broker, **failover** subscription per
shard) picks it up and runs the builder.

> Status: the builder + its work queue (`buildForLedger` / `buildAllClosed()` over
> `catalog.closedUnsegmented()`) are implemented and tested. Wiring the system‑topic trigger consumer
> into broker startup is the remaining integration point (today the builder is invoked directly / in
> tests). The catalog `state` machine already makes it idempotent and crash‑safe.

### 3.2 `StreamLakeSegmentBuilder.buildForLedger(dataLedgerId)` (idempotent)

```java
if (catalog.get(id).state == SEGMENTED) return;        // already done
if (segmentStore.covers(id)) { catalog.markState(id, SEGMENTED); return; } // partial-crash recovery

footers = pageIndex.footersFor(id);                    // METADATA only — never the data pages
pageEntryIds = footers.map(f -> f.dataEntryId);        // position i -> data entryId
perPage      = footers.map(f -> StreamLakeBatchStats.decode(f.stats));
for (col in the union of indexed columns) {            // one column segment per indexed column
    perPageCol = perPage.map(stats -> stats.column(col));   // that column's per-page ColumnStats
    columns.add(StreamLakeColumnSegment.build(col, type, perPageCol, segmentColumnMaxBytes, bloomFpp));
}
segmentStore.appendLedgerSegment(id, pageEntryIds, columns);   // (3.4) directory + column entries
catalog.markState(id, SEGMENTED);                      // last -> the durability barrier
```

Marking `SEGMENTED` **last** is the correctness barrier: a crash mid‑build leaves the ledger
`CLOSED`, so it is simply retried; `covers()` skips an already‑written segment on retry.

### 3.3 Building one column — `StreamLakeColumnSegment.build()` (no false negatives)

Per indexed column, over the ledger's pages:

* **min/max**: kept **per page** (`pMin[i], pMax[i]`) so a range/`BETWEEN`/`=` prunes to the exact page.
* **membership** (text/bytes only): a **per‑page bloom** — built from the page's exact set (or reusing
  its bloom) — so equality/`IN` prunes the exact page. Numeric columns rely on per‑page min/max.
* **collapse**: if the column's per‑page array would exceed `segmentColumnMaxBytes`, collapse to one
  whole‑segment stat — union min/max always, plus a union bloom when every page was low‑cardinality
  (recoverable exact sets); otherwise min/max only. A small text column (`name`) stays per‑page; a
  high‑cardinality one (`email`, whose per‑page blooms balloon) collapses. Never drops a matching page.

This is the *“array of min/max for numbers, array of bloom(set) for text, and one coarse filter once
it hits the cap”* design — made concrete.

### 3.4 What a segment stores + `appendLedgerSegment()`

A `LedgerSegment = {dataLedgerId, pageEntryIds[], Map<columnIndex, StreamLakeColumnSegment>}`.
`appendLedgerSegment` writes the directory entry then one column entry per column (§2.4), rolling the
head if needed, and publishes it into the in‑memory `byLedger` map. The chain's ids are recorded in
the znode via `setSegmentLedgerIds`. **The directory is where a page's position maps to its data
`entryId`** — so a candidate page position from the column arrays reads the right data‑ledger entry.

---

## 4. READ PATH

### 4.1 SQL → plan — Apache Calcite (`StreamLakeSqlPlanner`)

`SqlParser.create(sql, config).parseQuery()` (case preserved), then translate the `SqlSelect` /
`SqlOrderBy` against the topic’s `StreamLakeSchema` into a `Plan`:

* `SELECT c1,c2 | *` → **projection** = column indexes into the full schema (`null` = all).
* `FROM <topic>` → table.
* conjunctive (`AND`‑only) `WHERE` of `= > >= < <= BETWEEN IN` (either operand order; literal‑on‑left
  flips the operator) → `StreamLakeScanPredicate`; strict `<`/`>` become **exclusive** bounds so the
  exact row filter is correct, not merely conservative.
* `ORDER BY col [ASC|DESC]` + `LIMIT k` → bounded **top‑K**.
* predicates on the configured **event‑time column** become the `[fromMs,toMs]` **date‑prune window**
  and are dropped from the row filter (date pruning already covers them).

Columns resolve case‑insensitively. Literals are read per column type (`getValueAs(Integer/Long/
Double/Boolean/String)`).

### 4.2 Predicate model — `StreamLakeScanPredicate`

A conjunction of `ColumnPredicate{columnIndex, type, lo, loInclusive, hi, hiInclusive, inValues}`
(all bounds order‑preserving encoded). Two evaluations:

* `matches(stats)` — **prune** a unit (a page footer). Per column: false if `lo > cs.max` or
  `hi < cs.min` (range miss), or if an `IN`/`=` set has **no** value with `cs.mightContain(v)`.
  Conservative — **never a false negative**.
* `matchesRow(row)` — **exact** filter a decoded row: re‑encode the cell and check inclusive/exclusive
  bounds + membership.

Builder: `.eq(idx,type,v)`, `.range(idx,type,lo,loIncl,hi,hiIncl)`, `.in(idx,type,values)`. Each
`ColumnPredicate` also drives per‑page segment pruning via `StreamLakeColumnSegment.candidatePositions`.

### 4.3 Pruning — `StreamLakePruner.prune(fromMs,toMs,predicate)`

Metadata‑only and conservative. Date → data ledgers, then straight to the exact pages of each ledger:

```
candidates = catalog.candidateLedgers(fromMs, toMs)          // TIER 1: date -> data ledgers
for each ledger:
    seg = segmentStore.segmentFor(ledger)
    if seg == null:                                          // not segmented yet (recent data)
        for footer in pageIndex.footersFor(ledger):          //   per-page footer prune (fallback)
            if predicate.matches(decode(footer)): keep (ledger, footer.dataEntryId)
        continue
    surviving = boolean[seg.numPages] all true               // TIER 2: exact-page prune from segment
    for cp in predicate.columns():                           //   AND each column's candidate pages
        cseg = seg.columns.get(cp.columnIndex())
        if cseg != null: surviving &= cseg.candidatePositions(cp)   // per-page range/bloom test
    for i where surviving[i]:                                //   position i -> data entryId
        keep PagePointer(ledger, seg.pageEntryIds[i])
```

Once a ledger is **segmented**, pruning reads **only** the in‑memory column segments — no per‑page
footer read — and lands directly on the candidate pages. `Stats` counters record
`candidateLedgers / segmentsTotal / segmentsSkipped / pagesScanned / pagesKept`.

### 4.4 Scan → read → exact filter — `StreamLakeQueryExecutor`

```java
List<Object[]> scan(fromMs, toMs, predicate):
    pages = pruner.prune(fromMs, toMs, predicate);
    for p in pages:
        arrow = pageReader.readArrowBatch(p.ledgerId, p.entryId);   // seam: read entry, strip framing
        for row in decoder.decodeRows(arrow):
            if predicate.matchesRow(row): rows.add(row);            // exact (pruning was conservative)
    return rows;
```

`PageReader` is the single broker/storage seam: in a broker it reads the managed‑ledger entry and
`StreamLakeBatchPayload.arrowBatch(...)` strips the framing back to Arrow.

### 4.5 Late materialization — `StreamLakeArrowBatchDecoder`

Because Arrow is columnar, the decoder can read **only the columns needed first**:
`decodeColumn(ipc, joinKeyCol)` / `decodeColumns(ipc, {predicate cols})` decode just those vectors;
the remaining projected columns are materialized for **surviving rows only**. `decodeRows(ipc)`
materializes everything when full rows are needed.

### 4.6 ORDER BY / LIMIT — `StreamLakeTopK`

A bounded heap of size `k` (O(N log k), O(k) memory): `offer(row)` keeps only the k best by the sort
column; `results()` returns them best‑first. `canSkipPage(pageExtreme)` lets a scan skip an entire
page once the heap is full and the page’s max (DESC) / min (ASC) can’t beat the current kth row —
combining with segment/page zone‑maps for early termination.

### 4.7 Consumer columnar deserialize — `StreamLakeConsumer`

For pub/sub delivery, wraps a raw `Consumer<byte[]>`: if `hasFooter`, `arrowBatch(payload)` →
`decoder.decodeRows` yields the original rows; a plain message is passed through. (This replaced the
old broker‑side read transcoder — deserialization is now client‑side.)

### 4.8 Full‑scan example

`SELECT id, salary FROM employee WHERE deptId = 1 AND salary >= 300000 ORDER BY salary DESC LIMIT 10`

1. Calcite → predicate `{deptId=1, salary≥300000}`, projection `[id,salary]`, sort `salary DESC`,
   limit 10, window `[MIN,MAX]`.
2. Date prune: all data ledgers (unbounded window) are candidates.
3. Exact‑page prune: for each segmented ledger, `deptId`'s per‑page min/max array keeps pages whose
   `[min,max]` spans `1`, `salary`'s per‑page min/max array keeps pages with `max ≥ 300000`; AND the
   two → the surviving page positions, mapped to data entryIds via the directory. (Unsegmented recent
   ledgers fall back to a per‑page footer prune.)
4. Read survivors’ Arrow, decode, keep rows with `deptId==1 && salary>=300000`.
5. Top‑K (k=10, salary DESC); project to `[id,salary]`.

---

## 5. INNER JOIN — end to end

`StreamLakeHashJoin` is a two‑phase broadcast hash join: the **smaller, already‑pruned** side is built
into a multimap keyed by the join column; the other side streams and probes, emitting
`concat(probeRow, buildRow)` per inner match. A `maxBuildRows` guard fails fast instead of OOM (swap
the on‑heap map for Chronicle Map / NVMe behind `addBuildRow` without changing the probe).

Worked example (the target query):

```sql
SELECT p.personId, p.name, p.age, e.companyName, e.deptId, e.salary
FROM Person p INNER JOIN Employee e ON p.personId = e.personId
WHERE p.age BETWEEN 50 AND 65
  AND p.createTime >= '2026-01-01' AND p.createTime < '2026-04-01'
  AND e.deptId BETWEEN 100 AND 150 AND e.salary >= 300000
  AND e.startDate >= '2025-01-01' AND e.endDate < '2026-01-01'
ORDER BY e.salary DESC LIMIT 100;
```

Executed by `scanInnerJoin(...)` (build = the smaller side after pruning):

**Phase 1 — build the smaller side (say Person, restricted by the tight `createTime` window):**
1. Plan Person with Calcite → predicate `{age∈[50,65]}`, and `createTime∈['2026-01-01','2026-04-01')`
   becomes the **date window** (dropped from the row filter).
2. `prune(window, {age∈[50,65]})`: Tier 1 keeps Person data ledgers overlapping Q1‑2026; Tier 2 skips
   segments whose `age` range misses `[50,65]`; Tier 3 keeps pages whose footer `age` range overlaps.
3. For each surviving page, **late‑materialize** `personId` (+ `age` for the exact filter), keep rows
   with `age∈[50,65]`, then materialize the projected `name,age`. `join.addBuildRow([personId, name,
   age])` keying on `personId` (bounded by `maxBuildRows`).

**Phase 2 — stream + probe the other side (Employee):**
4. Plan Employee → predicate `{deptId∈[100,150], salary≥300000}`; `startDate/endDate` become its date
   window.
5. `prune(...)` Employee the same way → surviving pages.
6. For each page, late‑materialize `personId` first; **probe** the Person table; only for a **hit** do
   we materialize the rest (`companyName,deptId,salary`) and emit
   `concat(employeeRow, personRow)`.

**Phase 3 — order + limit:** feed the joined stream into `StreamLakeTopK(k=100, salary DESC)`; project
to `[personId,name,age,companyName,deptId,salary]`.

Why it scales: both sides are pruned to *fresh candidate pages* **before** any join, so only rows that
survive pruning + row filter reach the join. The build side stays small (fits in a
128–256 GB broker, or spills to NVMe); the probe side streams — no distributed shuffle/sort like
Iceberg+Spark, and 1 MiB pages waste ~128× less read IO than 128 MiB Parquet row groups.

---

## 6. Worked byte‑level examples

**Page‑index ledger entry** for page `(dataLedgerId=100, dataEntryId=7)` describing a batch whose
`deptId∈{1,2}` and `salary∈[100,900]`:

```
0x46 ('F') | 00000000_00000064 (100) | 00000000_00000007 (7) |
  'SLS1' 01 00000002                      // footer: 2 indexed cols
  00000002 02 03 00000002                 //  col 2 (deptId) INT32, flags=min|set, distinct=2
    00000004 <min=1>  00000004 <max=2>
    00000002 00000004 <1> 00000004 <2>    //  exact set {1,2}
  00000004 01 01 00000009                 //  col 4 (salary) INT64, flags=min-only (high-card)
    00000008 <min=100> 00000008 <max=900>
```

**Segment for ledger 100** (a directory + one entry per indexed column):

```
0x44 ('D') | dataLedgerId=100 | numPages=3 | entryId[0..2] = 0,1,2   // position -> data entryId

0x43 ('C') | dataLedgerId=100 | blobLen=NN |                          // deptId (numeric, per-page)
  columnIndex=2 type=INT32 collapsed=0 numPages=3
    page0: flags=minmax min=1  max=1              // page 0 all dept 1
    page1: flags=minmax min=2  max=2
    page2: flags=minmax min=1  max=3

0x43 ('C') | dataLedgerId=100 | blobLen=MM |                          // email (text -> per-page bloom)
  columnIndex=3 type=STRING collapsed=0 numPages=3
    page0: flags=minmax|bloom min=a@x max=b@x bloom(...)
    page1: flags=minmax|bloom ...
    page2: flags=minmax|bloom ...
  // if this column's array exceeded segmentColumnMaxBytes: collapsed=1 with a single union stat
```

**Catalog entry** (41 bytes) for a closed, segmented ledger:

```
dataLedgerId=100 | createTs=1700000000000 | minEt=1700000000000 | maxEt=1700003600000 |
rowCount=9000000 | state=2 (SEGMENTED)
```

---

## 7. Isolation & scale

* **Metadata isolation:** page‑index / segment / catalog ledgers pin to a separate bookie affinity
  group (`metadataBookieAffinityGroup`) with high RF (e.g. RF‑50) so pruning reads scale horizontally
  without loading the pub/sub bookies. Metadata is small (footers, not data), so many copies are
  cheap.
* **Compute isolation:** query‑executor + segment‑build roles run on separate brokers
  (`queryExecutorEnabled`) with their own hardware.
* **Durability:** everything is a durable BK ledger; nothing critical lives only in broker memory.
  Cold segments can offload to object storage (`segmentOffloadEnabled`). Pulsar geo‑replication
  carries the table across regions.
* **Numbers:** 5–10K msg/s × 1 KiB, 1 KiB→1 MiB pages ⇒ ~5–10 pages/s; a 9M‑record ledger ≈ 9K pages.
  Broker holds only ~24 bytes of `Ref` per page in memory; footer bodies + segments live in ledgers.
  A scan prunes date → segment (skip 1000‑page runs) → page, reading only surviving 1 MiB pages.

---

## 8. Change log

**Client (pulsar-client)** — `StreamLakeProducer`, `StreamLakeArrowBatchEncoder/Decoder`,
`StreamLakeSchema`/`StreamLakeType`, `StreamLakeStatsBuilder`/`StreamLakeBatchStats`/`StreamLakeBloom`/
`StreamLakeOrderPreserving`, `StreamLakeBatchPayload`, `StreamLakeConsumer`, `StreamLakeScanPredicate`,
`StreamLakeColumnSegment`, `StreamLakeHashJoin`, `StreamLakeTopK`.

**Broker (pulsar-broker)** — `PersistentTopic` dumb‑pipe write + footer slice; metadata
`StreamLakeMetaStore` / `StreamLakeCatalog` / `StreamLakePageIndex` / `StreamLakeSegmentStore`; build
`StreamLakeSegmentBuilder`; query `StreamLakePruner` / `StreamLakeSqlPlanner` (Apache Calcite) /
`StreamLakeQueryExecutor`; policy `StreamingLakeConfig`.

**Removed (phase G — the dropped prototype)**
* Broker prototype: `StreamLakeBatcher/BatchPage/ColumnCodec/RangeBuilder/Transcoder/PageScan/Store/
  Join/DateIndex`, `StreamingLakeScanExecutor/Predicate/ScanService`, `SegmentSummary(Codec)`,
  `ColumnarPage`, `DateIndexLedger` + their wiring.
* Bookie “range API”: `PAGE_STATS`/`PAGE_PRUNE` RPCs + processors, RocksDB `PageRangeIndex`/
  `PageStatEntry`/`PageRangeCodec`, `LedgerStorage.recordPageRanges/giveIndexPages/scanPageStats`, the
  `AddRequest.pageRanges` field and the whole `addEntry(pageRanges)` chain (`LedgerHandle`/
  `PendingAddOp`/`BookieClient(Impl)`/`PerChannelBookieClient`), and `ManagedLedger.asyncAddEntry(
  pageRanges)`. (BookKeeper fork rebuilt so the regenerated proto drops these.)
* Client↔broker `CommandScan`/`CommandScanResponse` RPC (proto + `Commands`/`PulsarDecoder`/`ServerCnx`/
  `ClientCnx`) and the old prototype‑only `StreamingLakeConfig` knobs.
