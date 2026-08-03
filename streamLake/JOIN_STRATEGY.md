# StreamLake — Query Execution Strategy (joins, sort, aggregation)

How the StreamLake query broker executes analytical queries **without OOM**, how it **chooses** an
operator from cost statistics, and how it handles **skew**. The query broker runs on dedicated NVMe
hardware (no bookie co-located), so disk-spill operators (spill files, RocksDB) have the machine's
disk/IO to themselves.

Status legend: ✅ implemented & tested · 🚧 planned (design below).

---

## 1. The result is always streamed (no response OOM) ✅

A query result can be many GB (a many-to-many join can even exceed either input). The whole path is
**streaming**, so neither broker nor client buffers the result:

- Executor emits each row to a `RowConsumer` sink (`scan(...)`, `scanInnerJoin(...)`) — never a `List`.
- `StreamLakeQueryCoordinator.prepare(sql)` plans + resolves tables up front (clean **400** before any
  bytes), returns a `Prepared` whose `stream(sink)` pushes rows as produced.
- REST (`POST /admin/v3/streamlake/{tenant}/{ns}/query`) writes **NDJSON** via `StreamingOutput`
  (line 1 = column names, each next line = one row as a JSON array), flushing every 1024 rows.
- Admin client + `pulsar-admin streamlake query` read the NDJSON stream incrementally.

Result: broker heap holds ~one row at a time. (`StreamLakeSqlJoinQueryTest` verifies 1,320 rows over
the streaming REST path and via `prepare().stream()`.)

---

## 2. Cost statistics (the planner's inputs) ✅ (size) · 🚧 (cardinality/skew)

All estimates are **metadata-only** — no data pages are read.

✅ **Pruned size** — `StreamLakeStatistics.estimate(from, to, predicate)` runs the pruner
(date → segment → page) and returns `{pages, ledgers, rows≈pages·rowsPerPage, bytes≈pages·pageBytes}`.
Page count is exact; rows/bytes use `estimatedRowsPerPage` / `estimatedPageBytes` config. This is what
picks the build side and (later) the partition count. *(Verified: a selective personId range estimates
6 pages vs 300 for match-all.)*

🚧 **Distinct-key cardinality (HLL)** and **heavy hitters (top-K / Space-Saving)** per indexed column,
computed per page at the producer, stored in the page footer, and **merged at segment build** into
segment-level sketches. These drive hash-table sizing, **skew detection**, and result-size estimation
(`|L|·|R| / max(distinctL, distinctR)`, refined by heavy hitters).

---

## 3. Join operators

### 3.1 Broadcast / smaller-side-as-build hash join (#6) ✅

The default. `scanInnerJoin` is a two-phase broadcast hash join with **late materialization**: the build
side is inserted into a `StreamLakeJoinTable`; the probe side is streamed and only materializes a full
row on a key match. Now cost-aware:

- The coordinator **estimates both pruned sides** and **builds the smaller** one, streaming the larger
  as the probe (`JoinPlan.combineFromBuildLeft/Right` produce the natural `[left…, right…]` output for
  whichever side was built).
- The build table backend comes from the build side's topic config:
  - on-heap `OnHeapJoinTable` (default), or
  - ✅ **off-heap `SpillingJoinTable`** when `joinOffHeapEnabled` — row bytes spill to `joinSpillDir`,
    only a `key → (offset,length)` index stays on-heap; bounded by `joinMaxBuildRows`.
- 🚧 **Key-filter pushdown**: extract the small side's join keys into a Bloom/IN filter and push it into
  the big side's *pruning*, so most probe pages are skipped before being read.

*(`StreamLakeSqlJoinQueryTest` runs the join through the spilling table and returns 1,320.)*

Limit of #6 + spill: the spill **index** is O(build-row-count) on-heap, so a build side of billions of
rows still doesn't fit. That's what #4 and RocksDB address.

### 3.2 Grace / partitioned hash join (#4) ✅

For two large sides. Hash-partition **both** sides by `hash(key) % N` into N spill files in one pass;
then join partition `i` (load `build_i`, stream `probe_i`). Resident memory = O(total/N); pick N from
the pruned-size estimate so a partition fits the build budget. Pure sequential IO (suits the spill
disk). **Skew handling** (§4) is part of this operator.

### 3.3 RocksDB backend ✅ · sort-merge 🚧

- ✅ **RocksDB join table** (`RocksDbJoinTable`) — composite key `keyBytes|seq → rowBytes`, prefix
  scan; *both* keys and values on disk (only block cache/memtable off-heap). Selected via
  `joinStrategy=ROCKSDB` (force) or `joinLargeBuildUsesRocksDb` under AUTO. WAL off; off-heap capped.
- **Sort-merge join** — when inputs are already sorted or ORDER-BY/merge semantics are needed.

---

## 4. Skew handling (part of #4) 🚧

Plain Grace does **not** fix a single hot key (rehashing sends it to the same bucket). Using the
heavy-hitter sketch (§2):

1. Detect hot keys (frequency ≫ partition budget).
2. Non-hot keys → normal Grace partitioning.
3. Hot key, other side small → **broadcast** that key's small-side rows; stream the big side (map-side).
4. Hot key, both sides large → **salting**: expand the hot key on the big side `k → k#0..k#S` and
   **replicate** the small side's `k` rows across the salts; union results.
5. Many-key overflow (partition big due to many keys, not one) → **recursive re-partition** (new seed).
6. Both sides billions for the same key → the **output is quadratic**; the cost guard (§5) aborts it.

---

## 5. Operator selection + guards ✅ (skew 🚧)

A cost-based selector in `prepare()`:

```
prune both sides (metadata-only) -> estL, estR
build = argmin(estL, estR); probe = other
if est(build).bytes <= joinBuildMemoryBudget:   BROADCAST (#6)   [+ key-filter pushdown]
else:                                           GRACE (#4), N = ceil(est(build)/partitionBudget)
if estResultRows > runawayThreshold:            reject/abort (quadratic blowup guard)
config force_strategy = {broadcast|grace|rocksdb|sortmerge}   # escape hatch / tests
```

**`EXPLAIN`-lite** ✅: return/log the chosen operator, per-side estimates, partition count, and any
skew keys, so a plan is never a black box.

*(Implemented: the coordinator estimates both sides, builds the smaller, and picks BROADCAST vs GRACE
by `joinBuildMemoryBudget`; `EXPLAIN <query>` returns the plan. Skew handling (§4) is the remaining 🚧.)*

---

## 6. Sort & aggregation (RocksDB as the external sorted map) ✅

RocksDB (sorted LSM) is reused as one **external sorted-map** primitive:

- **ORDER BY** (no/large LIMIT) → write `sortKey → row`, iterate in order = external sort. Keep the
  existing bounded **top-K** for `ORDER BY … LIMIT k` (a k-heap is cheaper).
- **GROUP BY** → `groupKey → aggregate state` with a merge operator (sum/count/min/max/HLL) = a
  spillable aggregation hash table for when #groups exceeds memory. *(GROUP BY is a new planner feature —
  parse aggregates + group keys — on top of today's single-table SELECT/WHERE/ORDER BY/LIMIT + inner
  join.)*

Cap RocksDB off-heap memory; disable the WAL.

---

## 7. Config reference

| Key (`StreamingLakeConfig`, topic policy) | Default | Meaning |
|---|---|---|
| `queryReadConcurrency` | 16 | parallel page prefetch during a scan/join |
| `estimatedRowsPerPage` | 1000 | cost estimate: rows per page |
| `estimatedPageBytes` | 1 MiB | cost estimate: bytes per page |
| `joinOffHeapEnabled` | false | build table spills row bytes to a file |
| `joinMaxBuildRows` | 5,000,000 | build-side admission guard (fail fast vs OOM) |
| `joinSpillDir` | "" (JVM temp) | spill directory (point at the query broker's NVMe) |
| `joinBuildMemoryBudget` | 256 MiB | broadcast-vs-grace threshold |
| `joinMaxPartitions` | 256 | cap on Grace partition count |
| `runawayResultRows` | 0 (off) | abort quadratic-blowup queries |

---

## 8. Build order

1. ✅ Streaming results (no response OOM).
2. ✅ Statistics estimator (pruned size).
3. ✅ #6 broadcast smaller-side-as-build + off-heap spill wiring.
4. 🚧 HLL + heavy-hitter sketches (segment) → cardinality/skew/result-size.
5. 🚧 Selector + `EXPLAIN`-lite + runaway guard.
6. ✅ #4 Grace partitioned join · 🚧 skew (broadcast/salting) + key-filter pushdown.
7. 🚧 RocksDB external sort (ORDER BY) → GROUP BY (planner + aggregation).
8. ✅ RocksDB join backend (`joinStrategy` force) · 🚧 sort-merge backend.
