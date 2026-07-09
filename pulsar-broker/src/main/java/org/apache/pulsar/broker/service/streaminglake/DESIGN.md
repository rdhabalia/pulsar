# StreamLake — Detailed Design & Implementation

StreamLake turns a Pulsar topic into a **fresh‑data columnar table store** you can prune, scan,
filter, top‑K and inner‑join with SQL, while the pub/sub write path stays a dumb, low‑latency pipe.

**Division of labour**

* **Client** does the heavy work: batch rows → **Apache Arrow** columnar encoding → per‑batch pruning
  stats (min/max + exact‑set/bloom) → frame into one message.
* **Broker** is a *dumb pipe*: persist the client’s payload as a normal managed‑ledger entry, then
  (off the ack path) slice the trailing stats footer and append it to a **page‑index ledger**. It
  never parses Arrow, never computes ranges, never asks the bookie to evaluate predicates.
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
* a **segment** is a *merged* summary of a run of pages (union min/max + union exact‑set → cap →
  bloom), stored as one entry per page‑group — not per‑page arrays;
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

After the entry is durable, `addComplete` (already off the ack‑critical path) slices the footer and
appends it **asynchronously** on the broker executor:

```java
if (pageIndex != null && StreamLakeBatchPayload.hasFooter(entryData)) {
    byte[] footer  = StreamLakeBatchPayload.statsFooter(entryData);   // tail slice, no Arrow parse
    long   ledger  = position.getLedgerId();
    long   entry   = position.getEntryId();
    executor.execute(() -> pageIndex.appendFooter(ledger, entry, footer));  // (1.8)
}
```

Best‑effort: if a footer append is lost, a scan falls back to the message’s own footer.

### 1.8 Page‑index ledger append — `StreamLakePageIndex.appendFooter()`

```java
byte[] entry = encode(dataLedgerId, dataEntryId, footer);   // 'F'|dataLid(8)|dataEid(8)|footer
ensureHeadFor(entry.length);                                 // roll head if > maxHeadBytes (4 MiB)
long piEntryId = addToHead(entry);                           // append to head BK ledger
refsByDataLedger.get(dataLedgerId).add(new Ref(head.getId(), piEntryId, dataEntryId));  // in-mem
```

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

A chain of BK ledgers (ids in the znode). Entry:

```
'S'(1) | dataLedgerId:int64 | startEntry:int64 | endEntry:int64 | statsLen:int32 | <merged StreamLakeBatchStats>
```

Unlike the page index, **segments are held in memory** (`byLedger: dataLedgerId → [Segment]`) — they
are small and are the hot pruning layer; replayed fully on `open()`. RF is `segment{…}Quorum`
(default 5/5/3, tunable much higher for read scaling).

---

## 3. ASYNC SEGMENT BUILD (after a data ledger closes)

A **segment** summarizes a contiguous run of a data ledger’s pages so a scan can skip whole entry
ranges before touching per‑page footers.

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
for (i = 0; i < footers.size(); i += pagesPerSegment) {
    group  = footers[i .. i+pagesPerSegment];
    parts  = group.map(f -> StreamLakeBatchStats.decode(f.stats));
    merged = StreamLakeStatsMerger.merge(parts, setMaxCardinality, bloomFpp);   // (3.3)
    segmentStore.appendSegment(new Segment(id, group.first.dataEntryId,
                                           group.last.dataEntryId, merged));    // (3.4)
}
catalog.markState(id, SEGMENTED);                      // last -> the durability barrier
```

Marking `SEGMENTED` **last** is the correctness barrier: a crash mid‑build leaves the ledger
`CLOSED`, so it is simply retried; `covers()` skips already‑written segments on retry.

### 3.3 Stats merge — `StreamLakeStatsMerger.merge()` (per column, no false negatives)

Group each column’s per‑page `ColumnStats`, then:

* **min/max**: union always (`min = min(all mins)`, `max = max(all maxs)`) → range skip at the segment.
* **exact sets**: if **every** contributing page had an exact set, union them:
  * union size ≤ `setCap` → keep the **exact set** (segment still prunes equality with no false
    positives);
  * union size > `setCap` → promote to **one segment‑level bloom** (`distinctCount` = union size).
* **high‑cardinality**: if any page already had a bloom (no set to union) → keep **min/max only**
  (`distinctCount = -1`); equality pruning for that column stays precise at the **per‑page** bloom.

This is the *“set(N) per page, bloom per segment; large fields → one coarse bloom”* rule from the
brainstorm, made concrete — and it never drops a page that could match.

### 3.4 What a segment stores + `appendSegment()`

A `Segment = {dataLedgerId, startEntry, endEntry, StreamLakeBatchStats(merged)}` — the merged stats
carry, per indexed column, `min/max` + (`exactSet` | `bloom` | neither). `appendSegment` encodes the
entry (§2.4), rolls the head if needed, appends to the head BK ledger, and publishes it into the
in‑memory `byLedger` map. The chain’s ids are recorded in the znode via `setSegmentLedgerIds`.

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

* `matches(stats)` — **prune** a unit (page footer or merged segment). Per column: false if
  `lo > cs.max` or `hi < cs.min` (range miss), or if an `IN`/`=` set has **no** value with
  `cs.mightContain(v)`. Conservative — **never a false negative**.
* `matchesRow(row)` — **exact** filter a decoded row: re‑encode the cell and check inclusive/exclusive
  bounds + membership.

Builder: `.eq(idx,type,v)`, `.range(idx,type,lo,loIncl,hi,hiIncl)`, `.in(idx,type,values)`.

### 4.3 Hierarchical pruning — `StreamLakePruner.prune(fromMs,toMs,predicate)`

Top‑down, metadata‑only, conservative:

```
candidates = catalog.candidateLedgers(fromMs, toMs)          // TIER 1: date -> data ledgers
for each ledger:
    segments = segmentStore.segmentsFor(ledger)
    if segments empty:                                       // not segmented yet
        for footer in pageIndex.footersFor(ledger):          //   full per-page prune
            if predicate.matches(decode(footer)): keep (ledger, footer.dataEntryId)
        continue
    matchedRanges = [ (seg.startEntry, seg.endEntry)         // TIER 2: segment -> [start,end]
                      for seg in segments if predicate.matches(seg.stats) ]
    if matchedRanges empty: continue                         //   whole ledger skipped
    for footer in pageIndex.footersFor(ledger):              // TIER 3: page within surviving ranges
        if inAnyRange(footer.dataEntryId, matchedRanges) and predicate.matches(decode(footer)):
            keep PagePointer(ledger, footer.dataEntryId)
```

Reads **only** metadata (catalog + segments in memory, page footers on demand). `Stats` counters
record `candidateLedgers / segmentsTotal / segmentsSkipped / pagesScanned / pagesKept`.

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
2. Tier 1: all data ledgers (unbounded window).
3. Tier 2: skip segments whose merged `deptId` set lacks `1` **or** `salary.max < 300000`.
4. Tier 3: within survivors, keep pages whose footer `deptId` set contains `1` and `salary.max ≥
   300000`.
5. Read survivors’ Arrow, decode, keep rows with `deptId==1 && salary>=300000`.
6. Top‑K (k=10, salary DESC); project to `[id,salary]`.

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

**Segment ledger entry** merging pages 0..1023 of ledger 100:

```
0x53 ('S') | ...=100 | start=0 | end=1023 | statsLen=NN |
  'SLS1' 01 00000002
    col2 deptId: flags=min|set, set = union {1,2,3}          // still ≤ cap -> exact
    col4 salary: flags=min-only, min=50, max=1_000_000, distinct=-1   // high-card -> min/max
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
`StreamLakeStatsMerger`, `StreamLakeHashJoin`, `StreamLakeTopK`.

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
