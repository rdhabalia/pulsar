# StreamLake: Bloom‑filter page index & inner join

This document explains **how** the bloom‑filter page pruning and the **broadcast hash inner join
with runtime semi‑join push‑down** are implemented, and gives copy‑paste steps to **build, run, and
validate** them. It is a companion to [`STREAMLAKE.md`](STREAMLAKE.md) (the end‑to‑end design).

- [1. Bloom filter in the bookie page index](#1-bloom-filter-in-the-bookie-page-index)
- [2. Inner join (broadcast hash + runtime semi‑join)](#2-inner-join-broadcast-hash--runtime-semi-join)
- [3. Build, run, and validate](#3-build-run-and-validate)

---

## 1. Bloom filter in the bookie page index

### Why
Min/max range pruning skips a page only when the predicate value can't fall in the page's
`[min,max]`. For a **semi‑join** the probe keys (the build side's join keys) may be *scattered* across
the whole domain — so the range is wide and prunes nothing, even though most pages contain *none* of
those specific keys. A per‑page **value bloom** prunes exactly that case: "could this page contain any
of these keys?"

### Pieces

**`BloomFilter`** — `bookkeeper-server/.../bookie/storage/ldb/BloomFilter.java`
A tiny, deterministic, self‑describing bloom over opaque `byte[]` values, so the **broker builds it at
write** and the **bookie tests it at prune** with identical hashing.
- `build(values, bitsPerElement)` → `byte[]` blob `{ k | mBits | bits }` (`mBits` a power of two).
- `mightContain(blob, value)` → `false` is definite, `true` may be a false positive.
- Hashing: FNV‑1a 64‑bit + avalanche, two derived hashes combined by Kirsch–Mitzenmacher double
  hashing (`h1 + i·h2`). No external dependency.

**`PageRangeCodec` extension** — `.../ldb/PageRangeCodec.java`
The blob format gains an optional, **backward‑compatible** extension section appended after the
existing range section:
```
[ range section (unchanged) ]
byte EXT_PRESENT
short numBloomColumns    per: columnId, bloomLen, bloomBytes     <- page side
short numKeySetColumns   per: columnId, numKeys, [len, bytes]*   <- predicate side
```
- `encodePage(ranges, blooms)` — page blob = per‑column min/max **plus** a per‑column value bloom.
- `encodePredicate(ranges, keySets)` — predicate blob = per‑column ranges **plus** a per‑column key set.
- `decodeAll(blob)` → `{ ranges, blooms, keySets }`. A plain range‑only blob simply has no extension
  (the existing `decode`/`encode`/`encodePage(Map<Short,Range>)` are unchanged).
- `pageCouldMatch(pageBlob, predicateBlob)` now requires, per predicate column, **both**:
  1. the **range** test (overlap; AND across columns, OR within a column) — as before; and
  2. the **key‑set** test — the page's bloom must possibly‑contain at least one probe key.
  A page that lacks a bloom (or a range) for a column is **conservatively kept**, so there are never
  false negatives and range‑only blobs behave exactly as before.

### Write path (broker builds the bloom)
`StreamLakeRangeBuilder.buildForBatch(config, messages)` already computes per‑column min/max from the
indexed columns; it now also collects each column's encoded values and builds a bloom, emitting
`PageRangeCodec.encodePage(ranges, blooms)`. That blob rides beside the entry in `addEntry` and the
bookie stores it in the RocksDB `page-ranges` column family (`recordPageRanges`).

### Prune path (bookie tests the key‑set)
`BookieClient.pagePrune(...)` → `giveIndexPages(ledgerId, start, end, predicateBlob)` iterates the
page index and calls `pageCouldMatch`, which tests each probe key against the page's stored bloom.
The bookie still never decodes a value or learns the schema — it only runs byte comparisons and bloom
checks.

> Tests: `PageRangeBloomTest` (no false negatives, low false‑positive rate, key‑set pruning, range∧
> bloom AND, round‑trip) and the existing `PageRangeIndexTest` / `StreamingLakePagePruneIntegrationTest`
> (range‑only back‑compat).

---

## 2. Inner join (broadcast hash + runtime semi‑join)

`SELECT o.orderId, c.region FROM Orders o JOIN Customers c ON o.customerId = c.customerId
 WHERE c.region='US-WEST' AND c.tier='gold' AND o.day IN [8,9]`

Runs **entirely in the broker** (the page index lives on the bookies; the client only submits the
query). Code: `StreamLakeJoin`, `StreamLakePageScan`.

### `StreamLakePageScan.scanRows(topic, bk, bounds, keySets, fromDate, toDate)`
The predicate scan, returning **rows with properties** (the join key and projected fields live in
message properties) plus stats:
- builds the predicate from `bounds` (ranges) **and** `keySets` (`encodePredicate`);
- date‑partition prunes ledgers, bookie `PAGE_PRUNE` prunes pages (range **and** bloom), then
  selectively decodes only matching rows;
- returns `RowResult { rows[{properties,value}], ledgersScanned, ledgersPrunedByDate, pagesRead }`.

### `StreamLakeJoin.innerJoin(Side a, Side b, bk)`
`Side = { topic, keyColumnId, keyProperty, filters, fromDate, toDate }`.

1. **Build side B** — `scanRows(B, filtersB)`; hash rows by the join key; collect the keys' **min/max**
   and the **key set** (`encodeKey`).
2. **Runtime semi‑join on A** — add to A's scan a range `Bound(key in [min,max])` (prunes pages by the
   page‑index min/max) **and** the **key set** under A's join column (prunes pages by each page's value
   bloom). A reads only pages whose key range/bloom can overlap B's keys.
3. **Probe** — `scanRows(A, filtersA + runtime filters)`; for each A row, look up its key in the hash
   and emit `JoinRow{ left=A.props, leftValue=A.value, right=B.props, rightValue=B.value }`.

Returns `JoinResult { rows, buildRows, probePagesRead, probeLedgersScanned, probeLedgersPrunedByDate }`.

### Why it's correct (and why the bloom is safe)
The **hash probe is the exact membership test**: an A row whose join key isn't in B produces no
output. Bloom false positives only cause a few extra *pages* to be read, never extra *result rows*.
So correctness is exact; the range+bloom only change how many pages are scanned.

### Memory / CPU
Memory is bounded by the (filtered) build side B; the probe side A streams page by page. CPU/I/O is
dominated by decoding the surviving A pages — exactly what the semi‑join filter shrinks. Build the
**smaller** side, make join keys **indexed on both sides**, and run heavy queries on a dedicated
query broker. (Full tables in [`STREAMLAKE.md` §5](STREAMLAKE.md#5-query-joins-and-broker-back-pressure).)

---

## 3. Build, run, and validate

Run from the Pulsar repo root (the directory with `build.gradle.kts` and `streamlake-demo.sh`).

### 3.0 Prerequisites
- JDK 17+, Maven, and the Gradle wrapper (`./gradlew`).
- The vendored BookKeeper must be installed to your local Maven repo so the broker resolves the
  bloom‑aware `PageRangeCodec`/`BloomFilter`. **After any BookKeeper change, reinstall it:**

```bash
# simplest: build + install the vendored BookKeeper to mavenLocal
./build-all.sh --bk-only

# (faster, targeted alternative — only the two changed modules)
cd bookkeeper && mvn -q -o install -pl bookkeeper-proto,bookkeeper-server -DskipTests \
  -P '!cargo-zigbuild' -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drat.skip=true \
  -Dspotless.check.skip=true -Denforcer.skip=true -Dmaven.javadoc.skip=true -Dsource.skip=true && cd ..
```

### 3.1 Bloom unit tests (bookie side, fast, no cluster)
```bash
cd bookkeeper
mvn -o test -pl bookkeeper-server -Dtest=PageRangeBloomTest -P '!cargo-zigbuild' -DfailIfNoTests=false \
  -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drat.skip=true -Dspotless.check.skip=true \
  -Denforcer.skip=true -Dmaven.javadoc.skip=true
cd ..
# expect: Tests run: 5, Failures: 0, Errors: 0
```

### 3.2 Inner‑join test on a real broker + real bookie (oracle‑checked)
This starts an **in‑process standalone cluster** (real Pulsar broker + real BookKeeper bookie, same
stack as `bin/pulsar standalone`), runs the Orders⋈Customers query, checks the result against a
brute‑force oracle, and asserts the bloom key‑set prunes Orders pages beyond the range alone.
```bash
./gradlew :pulsar-broker:test \
  --tests "org.apache.pulsar.broker.service.streaminglake.StreamLakeJoinTest"
# expect: BUILD SUCCESSFUL ; Tests run: 1, Failures: 0
```

### 3.3 Join demo — run it and see the output file
Starts the cluster, runs the same query, writes the joined rows + prune stats to a file, and stops the
cluster automatically.
```bash
# one demo (join only):
STREAMLAKE_OUT_DIR=/tmp/streamlake-out ./gradlew :pulsar-broker:test \
  --tests "org.apache.pulsar.broker.service.streaminglake.StreamLakeJoinDemo"

cat /tmp/streamlake-out/join-result.txt

# or run all demos (Person full-scan + predicate query + join) via the script:
./streamlake-demo.sh        # writes all-persons.txt, filtered-persons.txt, join-result.txt
```

**Expected `join-result.txt`:**
```
# Orders JOIN Customers ON customerId
# WHERE c.region='US-WEST' AND c.tier='gold' AND o.day IN [8,9]
# build side (gold US-WEST customers) = 4 rows
# Orders pages read: 4 of 40  (runtime semi-join range+bloom pruning)
# matched = 8 rows
order-0-8  ->  region=US-WEST
order-0-9  ->  region=US-WEST
order-120-8  ->  region=US-WEST
order-120-9  ->  region=US-WEST
order-180-8  ->  region=US-WEST
order-180-9  ->  region=US-WEST
order-60-8  ->  region=US-WEST
order-60-9  ->  region=US-WEST
```
The key line is **`Orders pages read: 4 of 40`** — the runtime semi‑join (min/max range + per‑page
bloom) read only the 4 pages that can contain the 4 gold US‑WEST customers, skipping the other 36.

### 3.4 Full StreamLake regression (optional)
```bash
./gradlew :pulsar-broker:test --tests "org.apache.pulsar.broker.service.streaminglake.*"
# expect: 15 tests, 0 failures (includes the join + both demos)
```
