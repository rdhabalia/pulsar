# StreamLake — Inner‑Join Benchmark & Cost Model

Companion to `DESIGN.md`. This documents a **runnable** inner‑join benchmark for StreamLake and a
**transparent cost model** comparing StreamLake to the common OSS analytics stack
**Kafka → Spark → S3 → Iceberg → Spark**.

> **Honest scope.** The StreamLake numbers below are **measured** on this branch by
> `StreamLakeJoinBenchmark` (a broker test). The competitor numbers are **modeled** from published
> list prices and each system's documented defaults — Spark/Iceberg/Kafka are *not* run here (no
> cluster in this environment). Every competitor figure is labeled *modeled* and every unit price is
> cited so you can re‑derive or update it. The goal is a defensible, reproducible apples‑to‑apples
> *methodology*, not a vendor benchmark.

---

## 1. What the benchmark measures

Harness: `pulsar-broker/src/test/.../streaminglake/StreamLakeJoinBenchmark.java`.

Query (the hard case from the design — a selective, fresh inner join):

```sql
SELECT ...
FROM   Person p INNER JOIN Employee e ON p.personId = e.personId
WHERE  p.eventTime IN [t0, t0+W)          -- date prune (5‑min data ledgers)
  AND  p.age    <= 52                      -- clustered predicate -> page prune
  AND  e.salary <= 63000                   -- clustered predicate -> page prune
```

Dataset shape (matches the request: **10 GB / 2 days / 5‑min ledger rollover**):

| Parameter | Value |
|---|---|
| Total logical data | 10 GB (Person 5 GB + Employee 5 GB) |
| Time span | 2 days, event‑time ordered |
| Data‑ledger rollover | every 5 min → **576 ledgers/table** |
| Page size | 1 MiB (**Parquet/Iceberg default data page is also 1 MB** — see §5) |
| Row size | ~1 KiB → 1024 rows/page |
| Layout | 4,608 pages/table, **9,437,184 total rows** |
| Build table | **off‑heap** `SpillingJoinTable`, spill dir = `pulsar/temp` (per request) |

### How it stays honest without materializing 10 GB
- **Metadata is built for the *full* layout** — every 5‑min ledger gets real page‑index footers
  (`StreamLakeStatsBuilder`), a real catalog entry with `[minEventTime,maxEventTime]`, and a real
  column‑oriented segment (`StreamLakeSegmentBuilder`). Metadata is small (tens of MB), so it is
  built in full in the mock BookKeeper. **Pruning is therefore exercised on the true layout.**
- **Data pages are synthesized on demand.** Only the pages that *survive pruning* are regenerated
  (deterministically) into real Arrow batches and decoded/joined. This is valid because pruning
  ratios and IO amplification are **layout‑driven and scale‑invariant** — a surviving 1 MiB page
  carries the same 1024 real rows whether or not its 9,999 sibling pages were ever written.
- **The off‑heap spill is real disk IO.** Surviving build rows are serialized through
  `StreamLakeRowCodec` and appended to a real `FileChannel` under `pulsar/temp`; the probe side reads
  them back on each key match. The file is deleted on `close()`.

Late materialization is real: the probe side reads **only the join‑key cell** per row and
materializes the full row **only on a hash‑table match** (`StreamLakeQueryExecutor.scanInnerJoin`).

---

## 2. Measured results (this branch, JDK 25, single broker JVM, `maxHeap=1300m`)

Command (see §7 to reproduce):

```
SL_BENCH_RUN=true SL_BENCH_TOTALGB=10 SL_BENCH_WINDOWHOURS=8 \
  ./gradlew :pulsar-broker:test --tests "*StreamLakeJoinBenchmark" \
  -x checkstyleMain -x checkstyleTest --no-daemon --no-build-cache --rerun-tasks
```

| Query window | Pages read (of 9,216) | **Data scanned** | **IO reduction** | Output rows | Build rows | Off‑heap spilled | Query latency |
|---|---|---|---|---|---|---|---|
| 8 h  | 832 | 832 MiB / 9.0 GB = **9.03 %** | **11×** | 412,672 | 425,997 | **14.6 MB** (real disk) | **1,110 ms** |
| 1 h  | 128 | 128 MiB / 9.0 GB = **1.39 %** | **72×** | 63,488 | 65,538 | — (on‑heap) | 148 ms |
| 1 h  | 128 | 128 MiB / 9.0 GB = **1.39 %** | **72×** | 63,488 | 65,538 | **2.2 MB** (real disk) | 187 ms |

Metadata build (full 576×2 ledger layout + footers + segments): **~13 s**, one‑time per run.

**Reading of the numbers**
- **IO amplification is the headline.** A selective join over fresh data reads **1–9 %** of the
  dataset. The reduction scales with selectivity: a 1‑hour window prunes **72×**; an 8‑hour window
  **11×**. This is the combination of 5‑min event‑time ledgers (date prune) and 1 MiB page pruning on
  the clustered predicate — exactly the fine granularity §5 argues for.
- **Off‑heap works and is cheap.** On‑heap 148 ms vs off‑heap 187 ms for the same 1‑hour query — the
  spill path adds **~26 %** latency but **removes the heap ceiling**: at the 8‑hour window the build
  side is 426 K rows and the join still completes in ~1.1 s with only 14.6 MB resident on disk,
  independent of broker heap. This is the mechanism that lets a modest broker join arbitrarily large
  build sides.
- **Late materialization pays off.** ~851 K rows are scanned across both sides for the 8‑hour query
  but full rows are built only for the 412 K matches on the probe side; non‑matching probe rows never
  allocate their wide columns.

---

## 3. Fair comparison methodology

Both systems can date‑prune (Iceberg via hidden partitioning; StreamLake via the catalog). To avoid
an unfair "5‑min ledgers vs day partitions" strawman, the comparison holds the **time granularity
equal** and contests the two places StreamLake actually differs:

1. **Within‑window block granularity.** StreamLake's prune/read unit is a **1 MiB page**. Iceberg's
   planning/read split defaults to **128 MiB** (`read.split.target-size = 134217728`) and its Parquet
   **row group defaults to 128 MB** (`write.parquet.row-group-size-bytes = 134217728`) — verified from
   the Iceberg docs (§8). Row‑group min/max is the primary skip unit in practice; that is **128×
   coarser** than a StreamLake page. Parquet *does* support 1 MB page‑level column‑index skipping, but
   only when the predicate column is **sorted within the file** and the engine uses column indexes —
   otherwise a matching row anywhere in a 128 MB row group forces the whole column chunk to be read.
2. **Join execution.** StreamLake runs an **in‑broker off‑heap hash join** (build side spills to local
   disk, probe streams) — **no network shuffle**. Spark performs a **distributed shuffle (sort‑merge
   or shuffle‑hash) join**: both filtered sides are repartitioned across the network, with shuffle
   write/read and spill. Even at equal row counts, Spark pays shuffle IO + cluster coordination +
   JVM/executor spin‑up.
3. **Freshness / copy cost.** StreamLake queries the live pub‑sub data **in place**. The competitor
   must first land Kafka → S3/Iceberg (a Spark Structured Streaming sink) and compact small files
   before the data is efficiently queryable — extra always‑on compute and minutes of latency.

**Modeled competitor IO for the reference 8‑hour query** (same 10 GB, event‑time layout):

| Layout assumption | Row‑group prune on predicate | Rows scanned | vs StreamLake (851 K) |
|---|---|---|---|
| StreamLake (measured) | 1 MiB page | **851 K** | 1× |
| Iceberg, hourly partitions, predicate **clustered/sorted** + column indexes | near page‑level | ~0.9–1.5 M | ~1–1.8× |
| Iceberg, hourly partitions, predicate **uncorrelated** (128 MB row group can't skip) | none within window | ~1.67 M | ~2× |
| Iceberg, **daily** partitions (typical to avoid small files) | none within day | ~5 M | ~6× |

StreamLake scans **2–6× fewer rows** for the same selective query purely from finer time + block
granularity, and avoids the shuffle entirely.

---

## 4. Cost model (transparent, list‑price based)

End‑to‑end **ingest + store + query** for one sustained pipeline. All unit prices are AWS on‑demand,
US‑East‑1, list price (see §8 for sources + dates — **verify current pricing before quoting**).

**Reference workload:** 10 MB/s sustained ingest (Person+Employee) = ~864 GB/day; 7 days queryable
"hot"; 30‑day total retention; **~1,000 selective inner‑join queries/day** over recent windows (the
§2 query profile).

### 4.1 StreamLake (one system: Pulsar + BookKeeper does ingest, storage, **and** query)

| Component | Sizing | Monthly (list) |
|---|---|---|
| Brokers (pub‑sub + query tier) | 3 × r5.2xlarge @ $0.504/hr × 730 | **$1,104** |
| Bookies (hot storage on NVMe) | 3 × i3en.2xlarge @ $0.904/hr × 730 (5 TB NVMe each) | **$1,980** |
| Cold offload (data > 7 d → S3) | ~19 TB × $0.023/GB‑mo | **$437** |
| **Query compute** | **marginal** — runs on already‑provisioned brokers (~1.1 s/query) | **$0** |
| **Total** | | **≈ $3,520/mo** |

Notes: RF‑3 is included in bookie sizing (3× bytes on NVMe for the hot tier). Queries add no
separate line item — the 1,000/day run on the same brokers that serve pub‑sub, at ~1 s each.

### 4.2 Kafka → Spark → S3 → Iceberg → Spark (five components)

| Component | Sizing | Monthly (list) |
|---|---|---|
| **Kafka** (MSK, 7‑day retention) | 3 × kafka.m5.2xlarge ≈ $0.84/hr × 730 + ~6 TB × $0.10/GB‑mo storage | **$2,439** |
| **Spark ingest** (Kafka→Iceberg sink, always‑on) | AWS's own EMR example: 3 × c4.2xlarge, 24/7 | **$1,102** |
| **S3 storage** (Iceberg data + metadata, 30 d) | ~26 TB × $0.023/GB‑mo + requests | **$598** |
| **Iceberg compaction** (periodic Spark) | ~½ of an always‑on small EMR cluster | **$550** |
| **Spark query** (1,000 joins/day) | EMR Serverless, ~$0.10 warm‑pool equivalent/query × 30 k | **$3,000** |
| **Total** | | **≈ $7,689/mo** |

### 4.3 Bottom line

| | StreamLake | Kafka+Spark+S3+Iceberg | Ratio |
|---|---|---|---|
| Monthly TCO (reference workload) | **~$3,520** | ~$7,689 | **~2.2× cheaper** |
| Components to operate | **1** (Pulsar/BK) | 5 | — |
| Query latency (selective join) | **~1 s** (measured) | seconds–minutes (cluster/shuffle/spin‑up) | — |
| Data freshness at query time | **live** | after Kafka→S3 sink + compaction | — |
| Data scanned per query | **1–9 %** (measured) | 2–6× more (modeled, §3) | — |

The dominant competitor cost is **query‑side Spark compute** (spin‑up/shuffle for each query) and the
**duplicated always‑on compute** across Kafka + two Spark roles. StreamLake collapses these into one
always‑on cluster where the query is a marginal ~1 s of CPU.

---

## 5. Where the competitor legitimately wins (don't oversell)

- **Cold, rarely‑queried archival at PB scale.** S3 is serverless at rest (~$0.023/GB‑mo, ~1.4×
  erasure overhead) while BookKeeper runs servers 24/7 and stores **3× bytes** at RF‑3. For data you
  almost never read, S3 is decisively cheaper — hence StreamLake's own §7 **offloads cold ledgers to
  object storage**. StreamLake's advantage is the **hot/warm, freshly‑queried tier**, not deep archive.
- **Massive output / full‑table scans.** If a query emits billions of rows or scans the whole table,
  Spark's horizontal scale‑out wins. StreamLake competes on **reducing the data that reaches the
  join**, not on out‑scanning Spark on huge outputs.
- **Mature ecosystem.** Iceberg has broad engine support (Trino, Flink, Snowflake, Dremio…). StreamLake
  is a single‑engine, fork‑local prototype.

Tiering resolves most of this: NVMe hot / HDD (st1 ~$0.045/GB‑mo) warm / S3 cold via bookie affinity
groups, so you pay 3× bytes only for the small hot set and object‑storage rates for the long tail.

---

## 6. Threats to validity (read before quoting)

- **Competitor is modeled, not run.** No Spark/Iceberg/Kafka executed here. The row‑scan and cost
  figures use documented defaults + list prices; real numbers vary with cluster tuning, file layout,
  caching, sort order and reserved/spot discounts.
- **StreamLake data pages are synthesized**, not stored — valid for *pruning ratios, IO amplification,
  join correctness and off‑heap spill behavior* (all layout‑driven), but it does **not** measure
  bookie read throughput/latency under a real 10 GB on disk, nor network. Absolute latency (1.1 s) is
  a single‑JVM in‑process figure; a distributed deployment adds RPC + bookie IO.
- **Uncompressed Arrow** in the harness. Real deployments would compress (Pulsar message compression),
  changing *bytes on disk* (not rows scanned). §3 compares on **rows/logical bytes** to stay
  compression‑neutral.
- **Prices are list, on‑demand, US‑East‑1, ~mid‑2024.** Verify current pricing; apply your
  reserved/savings‑plan/spot discounts (often 30–70 %) to both sides.
- **Clustered predicate.** The benchmark's predicate is clustered by page to exercise page pruning
  (as a well‑sorted analytics column would be). A fully random predicate prunes less on *both* systems
  (but StreamLake's finer block still helps); a highly selective equality on a high‑card indexed column
  prunes more (per‑page XOR).

---

## 7. Reproduce

```bash
cd pulsar/pulsar
# 10 GB / 2 days / 5‑min rollover, 8‑hour query window, OFF‑HEAP spill under pulsar/temp:
SL_BENCH_RUN=true SL_BENCH_TOTALGB=10 SL_BENCH_DAYS=2 SL_BENCH_ROLLOVERMIN=5 \
SL_BENCH_WINDOWHOURS=8 SL_BENCH_SPILLDIR="$PWD/temp" \
  ./gradlew :pulsar-broker:test --tests "*StreamLakeJoinBenchmark" \
  -x checkstyleMain -x checkstyleTest --no-daemon --no-build-cache --rerun-tasks --console=plain

# read the printed report (Gradle captures test stdout in the results XML):
python3 - <<'PY'
import re
f="pulsar-broker/build/test-results/test/TEST-org.apache.pulsar.broker.service.streaminglake.StreamLakeJoinBenchmark.xml"
print(re.search(r'===== StreamLake.*?(?=\]\]>|</system-out>)', open(f).read(), re.S).group(0))
PY
```

Config env vars (Gradle forwards the process environment to forked test JVMs; **`-D` is not
forwarded**, and `--no-daemon --no-build-cache --rerun-tasks` are required so the run isn't served
from cache):

| Env var | Default | Meaning |
|---|---|---|
| `SL_BENCH_RUN` | *(unset → test is a no‑op)* | must be `true` to run |
| `SL_BENCH_TOTALGB` | 10 | total logical dataset size |
| `SL_BENCH_DAYS` | 2 | event‑time span |
| `SL_BENCH_ROLLOVERMIN` | 5 | data‑ledger rollover (minutes) → ledger count |
| `SL_BENCH_WINDOWHOURS` | 8 | query time window (drives date pruning) |
| `SL_BENCH_BUCKETHI` | 30 | predicate selectivity (page‑pruning band) |
| `SL_BENCH_ONHEAP` | *(unset)* | `true` also runs an on‑heap pass for comparison |
| `SL_BENCH_SPILLDIR` | `pulsar/pulsar/temp` | off‑heap spill directory |

---

## 8. External anchors (verify before quoting)

**Verified live during authoring:**
- Apache Iceberg *Configuration* docs — `read.split.target-size` = **134217728 (128 MB)**,
  `write.parquet.row-group-size-bytes` = **134217728 (128 MB)**, `write.parquet.page-size-bytes` =
  **1048576 (1 MB)**, `read.parquet.vectorization.batch-size` = 5000.
  https://iceberg.apache.org/docs/latest/configuration/
- AWS EMR pricing *Example 1* — 3 × c4.2xlarge (1 master + 2 core), 730 hr, 100 % util =
  **$1,101.57/mo** ($330.60 EMR uplift + $871.62 EC2); EMR uplift ≈ **26 %** over EC2.
  EMR Serverless: **$0.052624**/vCPU‑hr, **$0.0057785**/GB‑hr. https://aws.amazon.com/emr/pricing/
- AWS MSK pricing — model confirmed: hourly broker instance + provisioned storage GB‑mo + per‑GB
  write. https://aws.amazon.com/msk/pricing/

**Canonical list prices used in §4 (US‑East‑1, on‑demand, ~mid‑2024 — the AWS pricing tables render
via JS and did not scrape; re‑confirm on the pricing pages):**
- S3 Standard **$0.023/GB‑mo** (first 50 TB); GET **$0.0004**/1k, PUT **$0.005**/1k.
- EBS **st1 (HDD) $0.045/GB‑mo**, **gp3 $0.08/GB‑mo**.
- MSK broker storage **$0.10/GB‑mo**.
- EC2 on‑demand: r5.2xlarge **$0.504/hr**, i3en.2xlarge **$0.904/hr**, kafka.m5.2xlarge **≈$0.84/hr**,
  c4.2xlarge **$0.398/hr** (EC2) + **$0.105/hr** (EMR) — the last two verified from the EMR example.

> No turnkey published study measures "fresh‑data selective inner‑join TCO" for either stack, so §4 is
> modeled transparently rather than cited from a single source. Swap in your own instance types,
> retention, query rate and discounts to localize it.
