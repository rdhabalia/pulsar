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

> Honesty note: registering a StreamLake table and submitting a query are today driven **inside the
> broker** (Track A, and the load/query helpers in Track B run in the same JVM as the broker in
> standalone). The **external admin/REST** register + query surface and the dedicated **query‑executor
> broker tier** (with its own local storage engine) are the next integration step — see §9 “Deferred”.

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

## 8. The query + expected result

The inner join the demo runs:

```
SELECT * FROM Person p JOIN Employee e ON p.personId = e.personId
WHERE p.age BETWEEN 30 AND 40 AND e.salary >= 40000
```

**Data model** (so the answer is checkable): row `i` has `Person.age = 20 + (i % 50)` and
`Employee.salary = 30000 + (i % 50) * 1000`, with `personId = i` shared by both tables. So a row
qualifies iff `i % 50 ∈ [10, 20]` (age 30–40 **and** salary ≥ 40000). Over `N = min(personRows,
empRows)` shared ids:

```
expected = count of i in [0, N) with (i % 50) in [10, 20]   # 11 of every 50
```

For `N = 6000` → **1,320** (the runner prints `expected=` and asserts `matches == expected`). The sample
row shows the concatenated `[empId, personId, salary, personId, name, age]`.

**Pruning at work**: the `age ∈ [30,40]` and `salary ≥ 40000` predicates prune whole segments/pages via
per‑page min/max before any data page is read; only surviving pages are fetched (in parallel, Phase D)
and row‑filtered exactly.

---

## 9. Validate the ledgers

**From the demo report (Track A)** — the per‑table block already lists the catalog ledger, the
page‑index ledger id(s), the segment ledger id(s), and every data ledger's state + offsets. That *is*
the ledger inventory.

**From `pulsar-admin` (Track B)** — inspect the managed (data) ledger chain of a topic:

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

- **External register + query surface (admin CLI / REST)**: today registration and query run in‑broker
  (Track A). A thin admin command + REST endpoint to (a) set a topic's `StreamingLakeConfig` and
  (b) submit SQL to the executor is the next step (the SQL planner + executor already exist:
  `StreamLakeSqlPlanner`, `StreamLakeQueryExecutor.executeSql`).
- **Dedicated query‑executor broker tier** with its own local storage engine (**RocksDB** vs mmap cache —
  under discussion) and object‑storage offload of cold segments.
- **500 GB+ load generator as a standalone client**: blocked on the external register path above; until
  then, scale the in‑broker runner via `SL_DEMO_PERSON_ROWS` / `SL_DEMO_EMP_ROWS`.

---

## 12. Cleanup / troubleshooting

- **No `SEGMENTED` ledgers**: you produced too few pages to close a data ledger. Ensure
  `#pages > managedLedgerMaxEntriesPerLedger` (Track A uses 50) — lower `SL_DEMO_ROWS_PER_PAGE` or raise
  the row counts.
- **`LedgerNotExistException` on query**: data ledgers were trimmed. StreamLake data must be retained;
  Track A sets **infinite retention** on the namespace. On a cluster, set
  `pulsar-admin namespaces set-retention <ns> --size -1 --time -1`.
- **Stop the server**: `sl-start.sh stop`. **Reset**: remove `$PULSAR_HOME/data` (zk + bk) between runs.
