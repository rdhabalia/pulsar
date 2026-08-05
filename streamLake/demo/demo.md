# StreamLake — Customer Demo Guide

This guide brings StreamLake up **end to end** and lets you verify a real inner‑join query over
columnar streaming data. It has two tracks:

- **Track A — Run it today, one command** (§3). A self‑contained, in‑broker runner that registers two
  tables (`Person`, `Employee`), generates a configurable load that creates **many** data / page‑index /
  segment ledgers, waits for the broker to auto‑build the catalog + segments, **prints the real
  ZooKeeper + catalog metadata**, runs the inner join, and prints **actual vs expected** results. This
  is the fastest way to show a customer the whole pipeline working and to validate the numbers.
- **Track B — Deploy to a remote Linux host** (§4–§8). Build a tar, ship it, batch‑configure multi‑TB
  bookie storage with **no manual file edits**, start `zk + bookie + broker`, generate load at scale,
  list the resulting ledgers, and run the query.

> Honesty note: **register**, **query**, and **info** are now first‑class external `pulsar-admin
> streamlake` commands (CLI → REST → broker; §8–§9), and a standalone **load generator** ingests at
> scale — so the whole of Track B (register → ingest → query → inspect) runs on a deployed server with
> **no in‑broker step**, driven by one script (`sl-demo.sh`). The dedicated **query‑executor broker
> tier** (with its own local storage engine) is the one remaining integration item — see §11.

---

## 1. What the demo proves

- **Client‑encoded columnar writes**: the producer encodes Arrow batches + a per‑page stats footer; the
  broker only appends bytes and slices the footer into a shared page‑index ledger (hot path stays thin).
- **Auto‑built, tiered metadata**: when a data ledger closes, the broker registers it in the **catalog**
  and rolls its page‑index footers into a compact **segment** (per‑page min/max arrays + low‑card exact
  sets + high‑card filters) — no manual build step.
- **Hierarchical pruning**: a query prunes **date → segment → page** before reading a single data page,
  then row‑filters exactly.
- **Bounded memory at scale**: segments load **on demand** from a catalog offset (LRU‑cached); only tiny
  pointers live in ZooKeeper. See `streamLake/QUERY_WALKTHROUGH.md` for the 10K‑ledger math.
- **Async, decoupled segment build (Phase F)**: optionally, closed‑ledger builds are dispatched to a
  system topic and built by a failover consumer off the pub‑sub broker.
- **Parallel page reads (Phase D)**: surviving pages are prefetched with bounded concurrency.

---

## 2. Prerequisites

- JDK 17+ (the fork builds/tests on JDK 25).
- The StreamLake fork checked out at `pulsar/pulsar` (branch `bk_lake_client`).
- For Track B: a remote Linux host with a small **NVMe** mount (bookie journal) and one or more
  **multi‑TB** mounts (bookie ledgers), plus `ssh`/`scp`.

---

## 3. Track A — Run the full pipeline in one command (verifiable)

From the repo root (`pulsar/pulsar`):

```bash
export SL_DEMO_RUN=true          # opt-in (the test is a no-op without this)
export SL_DEMO_PERSON_ROWS=6000  # rows in Person   (default 40000)
export SL_DEMO_EMP_ROWS=6000     # rows in Employee (default 40000)
export SL_DEMO_ROWS_PER_PAGE=20  # small pages => many data ledgers (default 20)
# export SL_DEMO_ASYNC_BUILD=true  # optional: build segments via the Phase F system topic

./gradlew :pulsar-broker:test --tests "*StreamLakeDemoRunnerTest" \
  -x checkstyleMain -x checkstyleTest --no-daemon --console=plain
```

It runs a **real broker against a real BookKeeper**, and prints a report like:

```
===== StreamLake demo =====
ingest: Person=6,000 rows, Employee=6,000 rows in 575 ms

Person metadata  (ZK node /streamlake/streamlake/ns/persistent/Person):
  catalogLedgerId   = 4
  pageIndexLedgerIds= [5]
  segmentLedgerIds  = [9]
  data ledgers: 6 total, 5 SEGMENTED
    data-ledger 2  SEGMENTED pages=50  seg=9[0..3]  pi=5[0..49]
    data-ledger 6  SEGMENTED pages=50  seg=9[4..7]  pi=5[50..99]
    data-ledger 7  SEGMENTED pages=50  seg=9[8..11]  pi=5[100..149]
    data-ledger 8  SEGMENTED pages=50  seg=9[12..15] pi=5[150..199]
    data-ledger 10 SEGMENTED pages=50  seg=9[16..19] pi=5[200..249]
    data-ledger 11 OPEN      pages=0   (open: not yet segmented)
… (Employee prints the same shape) …

query: Person(age 30..40) JOIN Employee(salary>=40000) ON personId
  matches=1,320 (expected=1,320)  latency=113 ms
  RESULT: OK
  sample row [empId,personId,salary,personId,name,age] = [9000002010, 2010, 40000, 2010, person-2010, 30]
```

**How to read it (this is the metadata model, live):**
- `catalogLedgerId` / `pageIndexLedgerIds` / `segmentLedgerIds` = the only things stored in ZooKeeper —
  tiny **pointers** at `/streamlake/<tenant>/<ns>/<topic>`.
- Each **data ledger** has a catalog entry: its state and — once `SEGMENTED` — the exact offsets used to
  load its metadata on demand: `seg=<segmentLedgerId>[start..end]` (5 entries: 1 directory + 4 columns)
  and `pi=<pageIndexLedgerId>[start..end]` (one footer per page).
- The **last** data ledger of each table is still `OPEN` (a live stream always has an open tail); it is
  still queryable via the page‑index fallback.
- `matches=1,320 (expected=1,320) … RESULT: OK` is the verifiable correctness check (see §8 for the
  formula).

Scale it up by raising the row counts (e.g. `SL_DEMO_PERSON_ROWS=2000000`). With
`SL_DEMO_ROWS_PER_PAGE=20` and the demo's `managedLedgerMaxEntriesPerLedger=50`, every 50 pages closes a
data ledger, so **#data ledgers ≈ rows / (rowsPerPage × 50)**.

---

## 4. Track B — Build a deployable tar

From the repo root:

```bash
streamLake/demo/scripts/sl-package.sh
# -> produces streamlake-demo.tar.gz (Pulsar distribution + this demo/ folder)
```

Ship it:

```bash
scp streamlake-demo.tar.gz user@HOST:/opt/
ssh user@HOST 'cd /opt && tar xzf streamlake-demo.tar.gz'   # unpacks apache-pulsar-<ver>/
```

### 4a. Track B — one command on the host (register → ingest → query)

After unpacking, a single interactive script does **everything**: it asks for your storage disk and the
two table sizes, configures + starts `zk + bookie + broker`, registers `Person` + `Employee`, ingests
the requested amount, prints the on‑storage layout, and shows the queries to run.

```bash
cd /opt/apache-pulsar-<ver>
streamlake-demo/scripts/sl-demo.sh
#   Local disk path for StreamLake storage [/mnt/nvme]: /mnt/nvme
#   Person table size in GB [500]: 500
#   Employee table size in GB [500]: 500
```

Non‑interactive / re‑runnable (raise the sizes any time — ingestion appends):

```bash
SL_STORAGE_DIR=/mnt/nvme SL_PERSON_GB=1000 SL_EMP_GB=1000 \
  streamlake-demo/scripts/sl-demo.sh
```

The steps are also runnable individually (all under `streamlake-demo/scripts/`, with `PULSAR_HOME` set):
`sl-configure.sh` (storage), `sl-start.sh start|stop|status`, `sl-register.sh`, `sl-ingest.sh`,
`sl-info.sh`, `sl-queries.sh [--run]`. The ingestion program is `streamlake-demo/ingest/StreamLakeIngest.java`
(compiled on the host against `$PULSAR_HOME/lib/*`); pass `SL_PERSON_GB`/`SL_EMP_GB` (target on‑disk size)
or edit it to use `--rows N`.

---

## 5. Track B — Batch‑configure multi‑TB storage (no manual edits)

On the host, point the **journal** at NVMe and the **ledgers** at your multi‑TB disk(s). The script
rewrites the managed keys idempotently — you never hand‑edit a conf file:

```bash
export PULSAR_HOME=/opt/apache-pulsar-<ver>
export SL_JOURNAL_DIR=/mnt/nvme/bk/journal              # small, fast NVMe (write-ahead log)
export SL_LEDGER_DIRS=/data1/bk/ledgers,/data2/bk/ledgers   # multi-TB HDD (comma-separated)
export SL_QUERY_LOCAL_DIR=/mnt/nvme/streamlake           # reserved for the query tier (see §7 Deferred)

streamLake/demo/scripts/sl-configure.sh
```

It sets, in both `conf/bookkeeper.conf` and `conf/standalone.conf`:
- `journalDirectory` → your NVMe path,
- `ledgerDirectories` → your multi‑TB path(s),
- `ledgerStorageClass=…DbLedgerStorage` (scales to the many page‑index/segment ledgers StreamLake
  creates),
and enables `systemTopicEnabled` + `topicLevelPoliciesEnabled` on the broker.

---

## 6. Track B — Start the server

```bash
export PULSAR_HOME=/opt/apache-pulsar-<ver>
streamLake/demo/scripts/sl-start.sh start     # zk + bookie + broker (standalone)
# wait until healthy:
until "$PULSAR_HOME/bin/pulsar-admin" brokers healthcheck; do sleep 2; done
streamLake/demo/scripts/sl-start.sh status    # -> healthy
# stop later with:  sl-start.sh stop
```

This brings up ZooKeeper + BookKeeper (bookie) + the StreamLake broker in one process. For a
multi‑bookie deployment, run real bookies and raise the StreamLake RF (see §7).

---

## 7. Configuration reference

**Bookie storage (real keys, set by `sl-configure.sh`)**

| Key | Where | Meaning |
|---|---|---|
| `journalDirectory` | `bookkeeper.conf`, `standalone.conf` | NVMe write‑ahead log (latency) |
| `ledgerDirectories` | same | multi‑TB bulk data (comma‑separated for multiple disks) |
| `ledgerStorageClass` | same | `DbLedgerStorage` — scales to many ledgers |

**Broker (real keys)**: `systemTopicEnabled=true`, `topicLevelPoliciesEnabled=true`,
`brokerDeleteInactiveTopicsEnabled=false`.

**StreamLake table config — this is a _topic policy_ (`StreamingLakeConfig`), not a broker‑global key.**
It is applied per topic. In Track A the runner sets it in‑broker; the external admin/REST path to set it
is §9 Deferred. Key fields:

| Field | Default | Meaning |
|---|---|---|
| `enabled`, `clientColumnarEnabled` | – | turn StreamLake on for the topic (client‑encoded path) |
| `columns` | – | schema + which columns are indexed (id order) |
| `setMaxCardinality` | 64 | low‑card columns kept as exact sets ≤ this; else a filter |
| `bloomFpp` | 0.01 | false‑positive rate for high‑card filters |
| `pageIndexMaxEntriesPerLedger` | 1,000,000 | page‑index ledger rollover (holds MANY data ledgers) |
| `segmentMaxEntriesPerLedger` | 200,000 | segment ledger rollover |
| `segmentCacheMaxEntries` | 512 | resident segment LRU (on‑demand load ⇒ bounded memory) |
| `queryReadConcurrency` | 16 | **Phase D** — max in‑flight page reads during a query |
| `asyncSegmentBuildViaSystemTopic` | false | **Phase F** — build segments via the system topic |
| `pageIndex*` / `segment*` Ensemble/WriteQuorum/AckQuorum | 3/3/2, 5/5/3 | per‑tier replication (RF) |
| `metadataBookieAffinityGroup` | "" | isolate StreamLake ledgers onto a separate bookie pool |

**Query‑executor local storage — Deferred.** `SL_QUERY_LOCAL_DIR` is created and reserved for the
dedicated query‑executor tier (segment cache spill / scratch). The storage engine for that tier
(e.g. **RocksDB** vs a flat memory‑mapped cache) is still under discussion, so it is **not yet a wired
broker key**; today segment caching is in‑JVM bounded LRU (`segmentCacheMaxEntries`).

---

## 8. The query — via `pulsar-admin` CLI (SQL) + expected result

The query is now a first‑class CLI command that submits SQL to the broker and prints the rows. The
broker plans it (`StreamLakeSqlPlanner`), runs it on the query coordinator (single‑table scan or a
two‑table **inner equi‑join**), and returns columns + rows:

```bash
$PULSAR_HOME/bin/pulsar-admin streamlake query streamlake/ns \
  "SELECT * FROM Person p JOIN Employee e ON p.personId = e.personId
   WHERE p.age BETWEEN 30 AND 40 AND e.salary >= 40000"
```

Output (a table; add `--json` for raw JSON):

```
Person.personId | Person.name | Person.age | Employee.empId | Employee.personId | Employee.salary
2010 | person-2010 | 30 | 9000002010 | 2010 | 40000
2011 | person-2011 | 31 | 9000002011 | 2011 | 41000
…
(1,320 rows, 34 ms)
```

The path is `pulsar-admin streamlake query` → `PulsarAdmin.streamLake().query(...)` →
`POST /admin/v3/streamlake/{tenant}/{namespace}/query` → broker coordinator → `scanInnerJoin`. Table
names in the SQL are the topics in the namespace. (Single‑table queries work too, e.g.
`SELECT name, age FROM Person WHERE age BETWEEN 30 AND 40`.)

> The **same** query runs through the in‑broker demo runner (Track A) — which additionally *registers*
> the tables and *loads* the data. Today a topic must be registered as a StreamLake table in‑broker
> (Track A) before the CLI can query it; the external *register* command is the last remaining gap
> (§11). The CLI **query** itself is fully wired and verified.

**Data model** (so the answer is checkable): row `i` has `Person.age = 20 + (i % 50)` and
`Employee.salary = 30000 + (i % 50) * 1000`, with `personId = i` shared by both tables. So a row
qualifies iff `i % 50 ∈ [10, 20]` (age 30–40 **and** salary ≥ 40000). Over `N = min(personRows,
empRows)` shared ids:

```
expected = count of i in [0, N) with (i % 50) in [10, 20]   # 11 of every 50
```

For `N = 6000` → **1,320**. This is asserted end‑to‑end by `StreamLakeSqlJoinQueryTest` both through the
coordinator **and** through the admin REST endpoint (the CLI's transport).

**Pruning at work**: the `age ∈ [30,40]` and `salary ≥ 40000` predicates prune whole segments/pages via
per‑page min/max before any data page is read; only surviving pages are fetched (in parallel, Phase D)
and row‑filtered exactly.

---

### Register a table externally (no in‑broker step)

```bash
$PULSAR_HOME/bin/pulsar-admin streamlake register streamlake/ns Person \
  --schema "personId:INT64,name:STRING,age:INT32" --rf 1
$PULSAR_HOME/bin/pulsar-admin streamlake register streamlake/ns Employee \
  --schema "empId:INT64,personId:INT64,salary:INT64" --rf 1
```
`--schema` is `name:TYPE[:noidx]` (INT32|INT64|DOUBLE|BOOLEAN|STRING|BYTES; indexed by default). Other
flags: `--rf` (replication, 1 for one bookie), `--page-index-max-entries`, `--segment-max-entries`,
`--max-cardinality`, `--bloom-fpp`, `--async-build`. (`sl-register.sh` runs both; the topic must exist
and have infinite retention — the script sets it.)

### The three demo queries + a validation scan

All four are bounded so they return quickly and print as a table (add `--json` for raw). `sl-queries.sh`
prints them ready to paste; `sl-queries.sh --run` executes them.

```bash
Q="$PULSAR_HOME/bin/pulsar-admin streamlake query streamlake/ns"

# 1) Inner join (Person ⋈ Employee ON personId), over a pruned personId slice
$Q "SELECT * FROM Person p JOIN Employee e ON p.personId = e.personId \
    WHERE p.personId BETWEEN 0 AND 100000 AND p.age BETWEEN 30 AND 40 AND e.salary >= 40000"

# 2) Group by (age -> COUNT/MIN/MAX; scans a slice, returns 50 rows)
$Q "SELECT age, COUNT(*), MIN(personId), MAX(personId) FROM Person \
    WHERE personId BETWEEN 0 AND 1000000 GROUP BY age"

# 3) Order by (top-20 highest salaries; bounded top-K external sort)
$Q "SELECT personId, salary FROM Employee \
    WHERE personId BETWEEN 0 AND 1000000 ORDER BY salary DESC LIMIT 20"

# 4) Single-table scan to validate data (a key-range slice; page-pruned)
$Q "SELECT personId, name, age FROM Person WHERE personId BETWEEN 0 AND 20"
```

Every query prints a **stats footer** (rows read, pages scanned/kept/pruned, bytes read, peak buffer,
elapsed) after the table. A whole‑table group‑by works too (heavy scan, 50‑row result):
`$Q "SELECT age, COUNT(*) FROM Person GROUP BY age"`. Event‑time (date) pruning is automatic by publish
time — the covered range is shown by `streamlake info` (§9); range predicates on indexed columns (e.g.
`personId BETWEEN …`) prune to just the pages in that slice, which is the "scan a range to validate"
capability.

## 9. Validate the ledgers

**From the demo report (Track A)** — the per‑table block already lists the catalog ledger, the
page‑index ledger id(s), the segment ledger id(s), and every data ledger's state + offsets. That *is*
the ledger inventory.

**From `pulsar-admin streamlake info` (Track B)** — the on‑storage layout at a glance (this is the
`sl-info.sh` output):

```bash
$PULSAR_HOME/bin/pulsar-admin streamlake info streamlake/ns Person
# StreamLake table: persistent://streamlake/ns/Person
#   data ledgers      : 214 total  (212 segmented, 2 not-yet-segmented)
#   page-index ledgers: 3  ids=[5, 88, 171]
#   segment ledgers   : 1  ids=[9]
#   catalog ledger    : 4
#   data pages        : 10,700 (data-ledger entries; rows = pages x rowsPerPage)
#   event-time range  : 1,700,000,000,000 .. 1,700,000,600,000 (epoch ms)
```

`data pages` is the number of data‑ledger entries; the **exact ingested row count** is echoed by the
ingestion program (`DONE Person: wrote N rows …`). `page-index` / `segment` / `catalog` are the only
StreamLake ledgers kept in ZooKeeper (as tiny pointers under `/streamlake/<tenant>/<ns>/<topic>`).

**From `pulsar-admin topics stats-internal` (Track B)** — inspect the managed (data) ledger chain:

```bash
$PULSAR_HOME/bin/pulsar-admin topics stats-internal persistent://<tenant>/<ns>/Person
# "ledgers": [ {ledgerId, entries, size}, ... ]  <- the data ledgers
```

The StreamLake page‑index / segment / catalog ledgers are internal BookKeeper ledgers pointed to by the
ZK node `/streamlake/<tenant>/<ns>/Person` (values shown in the Track A report). The count math for a
large deployment (e.g. 10K data ledgers → **500** page‑index ledgers, **1** segment ledger, **1**
catalog ledger, and a few‑KB ZK node) is worked out in `streamLake/QUERY_WALKTHROUGH.md`.

---

## 10. Async segment build (Phase F) demo

Set `SL_DEMO_ASYNC_BUILD=true` in Track A (or `asyncSegmentBuildViaSystemTopic=true` in the topic
policy). On data‑ledger close the broker publishes `{dataTopic, dataLedgerId}` to the per‑namespace
system topic `__streamlake_segment_build`; a **failover** consumer resolves the builder, builds the
segment (idempotent — safe to redeliver), and acks. The join result is identical; the heavy build is now
off the pub‑sub broker. `StreamLakeAsyncSegmentBuildTest` asserts the requests flow through the system
topic and the ledgers still reach `SEGMENTED`.

---

## 11. Deferred (next integration steps)

- **DONE — external query** (`pulsar-admin streamlake query` → REST → coordinator; §8).
- **DONE — external register** (`pulsar-admin streamlake register` → REST sets the topic's
  `StreamingLakeConfig`; §8, `sl-register.sh`). A deployed cluster now registers **and** queries with no
  in‑broker step; verified by `StreamLakeSqlJoinQueryTest.registerAndInfoViaAdmin`.
- **DONE — external info** (`pulsar-admin streamlake info` → REST reports the ledger inventory; §9).
- **DONE — standalone load generator** (`streamlake-demo/ingest/StreamLakeIngest.java`, driven by
  `sl-ingest.sh`): ingests Person + Employee to a configurable on‑disk size (GB) via the public client
  API; re‑runnable to grow the tables.
- **Dedicated query‑executor broker tier** with its own local storage engine (**RocksDB** vs mmap cache —
  under discussion) and object‑storage offload of cold segments. *(still open)*
- **Explicit event‑time window on `query`** (`--from/--to` epoch‑ms): today date pruning is automatic by
  publish time, and range predicates on indexed columns bound a scan; an explicit time‑window flag is a
  small follow‑up. *(still open)*

---

## 12. Cleanup / troubleshooting

- **No `SEGMENTED` ledgers**: you produced too few pages to close a data ledger. Ensure
  `#pages > managedLedgerMaxEntriesPerLedger` (Track A uses 50) — lower `SL_DEMO_ROWS_PER_PAGE` or raise
  the row counts.
- **`LedgerNotExistException` on query**: data ledgers were trimmed. StreamLake data must be retained;
  Track A sets **infinite retention** on the namespace. On a cluster, set
  `pulsar-admin namespaces set-retention <ns> --size -1 --time -1`.
- **Stop the server**: `sl-start.sh stop`. **Reset**: remove `$PULSAR_HOME/data` (zk + bk) between runs.
