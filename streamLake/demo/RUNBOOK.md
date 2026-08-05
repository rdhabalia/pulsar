# StreamLake Demo — End‑to‑End Runbook

A single, linear guide to run the StreamLake demo on a remote host: **build the tar → ship it → start
the server → register tables → ingest → query**. Every command is copy‑paste ready. For the *why* (the
metadata model, pruning, design), see `demo.md`; this file is the operational path.

> **Two machines.** The **build host** (your dev box with the fork checked out) produces the tar. The
> **remote host** (the box with the 10 TB NVMe) runs the server + demo. They can be the same machine.

---

## 0. What you get

One command on the remote host (`sl-demo.sh`) will:
1. ask for your **storage disk** and the **two table sizes**,
2. configure + start **ZooKeeper + BookKeeper bookie + broker** (Pulsar standalone),
3. **register** `Person` + `Employee` as StreamLake tables (schema + tuning),
4. **ingest** the requested amount of data (client‑encoded columnar),
5. print each table's **on‑storage layout** (data / page‑index / segment / catalog ledgers),
6. show the **inner‑join / group‑by / order‑by** queries + a validation **scan** to run.

---

## 1. Prerequisites

**Build host**
- JDK 17+ (the fork builds on JDK 25) and the Gradle wrapper (`./gradlew`) — no separate install.
- This fork checked out at `pulsar/pulsar` (branch `bk_lake_client`).

**Remote host**
- Linux, JDK 17+ (`java -version`, `javac -version` — `javac` is needed to compile the tiny load
  generator on the host).
- One NVMe/SSD mount with enough space: **~2× your total table size** (data ledgers + StreamLake
  metadata ledgers). For 500 GB + 500 GB, reserve ≥ ~1.2 TB; your 10 TB NVMe is plenty.
- `ssh`/`scp` access from the build host.

---

## 2. Step 1 — Generate the deployable tar (build host)

From the repo root (`pulsar/pulsar`):

```bash
streamLake/demo/scripts/sl-package.sh
```

This runs `./gradlew :distribution:pulsar-server-distribution:assemble` (compiles the whole fork —
several minutes the first time), then bundles the demo assets into the distribution. Output:

| Artifact | Path |
|---|---|
| **Deployable tar (ship this)** | `pulsar/pulsar/streamlake-demo.tar.gz` |
| Intermediate server distribution | `distribution/server/build/distributions/apache-pulsar-<ver>-bin.tar.gz` |

The tar unpacks to `apache-pulsar-<ver>/` containing the full server (broker + bookie + zk +
`bin/pulsar-admin` with the `streamlake` CLI) **and** `streamlake-demo/` (this runbook, `demo.md`, the
`scripts/`, and the `ingest/` load generator).

---

## 3. Step 2 — Ship it + unpack (remote host)

```bash
scp streamlake-demo.tar.gz user@HOST:/opt/
ssh user@HOST 'cd /opt && tar xzf streamlake-demo.tar.gz'      # -> /opt/apache-pulsar-<ver>/
```

---

## 4. Step 3 — Run the whole demo with ONE command (remote host)

```bash
ssh -t user@HOST
cd /opt/apache-pulsar-*/
streamlake-demo/scripts/sl-demo.sh
```

It prompts (press Enter for the defaults):

```
Local disk path for StreamLake storage (zk + bookie journal/ledgers) [/mnt/nvme]: /mnt/nvme
Person table size in GB [500]: 500
Employee table size in GB [500]: 500
```

**Non‑interactive** (skip prompts; also how you re‑run at a larger size):

```bash
SL_STORAGE_DIR=/mnt/nvme SL_PERSON_GB=500 SL_EMP_GB=500 \
  streamlake-demo/scripts/sl-demo.sh
```

When it finishes it prints the ingestion summary and the queries to run (§7–§8).

> **Big‑RAM host tip:** before the first run, raise the JVM sizes for a multi‑TB ingest by editing
> `conf/pulsar_env.sh` (`PULSAR_MEM="-Xms8g -Xmx8g -XX:MaxDirectMemorySize=16g"`) and `conf/bkenv.sh`
> (`BOOKIE_MEM="-Xms4g -Xmx4g -XX:MaxDirectMemorySize=24g"`). Defaults are 2 g and work for smaller runs.

---

## 5. What `sl-demo.sh` does (and how to run each step yourself)

`sl-demo.sh` is just an orchestrator over the scripts in `streamlake-demo/scripts/`. To run the phases
manually, first set `PULSAR_HOME`:

```bash
export PULSAR_HOME=/opt/apache-pulsar-<ver>
```

**5.1 Configure storage + broker keys** (idempotent; no hand‑editing conf files):
```bash
SL_JOURNAL_DIR=/mnt/nvme/bk/journal \
SL_LEDGER_DIRS=/mnt/nvme/bk/ledgers \
SL_QUERY_LOCAL_DIR=/mnt/nvme/streamlake \
  streamlake-demo/scripts/sl-configure.sh
```
Sets `journalDirectory`, `ledgerDirectories`, `ledgerStorageClass=DbLedgerStorage` (scales to many
ledgers) and enables `systemTopicEnabled` + `topicLevelPoliciesEnabled` on the broker.

**5.2 Start the server** (zk + bookie + broker; data on the disk):
```bash
SL_ZK_DIR=/mnt/nvme/zk SL_BK_DIR=/mnt/nvme/bk \
  streamlake-demo/scripts/sl-start.sh start
until "$PULSAR_HOME/bin/pulsar-admin" brokers healthcheck; do sleep 2; done   # wait until healthy
```

**5.3 Register the two tables** (creates the topics, infinite retention, StreamLake policy):
```bash
streamlake-demo/scripts/sl-register.sh
```

**5.4 Ingest** (compiles the load generator against `lib/*`, then loads to the target size):
```bash
SL_PERSON_GB=500 SL_EMP_GB=500 streamlake-demo/scripts/sl-ingest.sh
```

**5.5 Inspect the storage layout:**
```bash
streamlake-demo/scripts/sl-info.sh
```

**5.6 Show / run the queries:**
```bash
streamlake-demo/scripts/sl-queries.sh          # print copy‑paste commands
streamlake-demo/scripts/sl-queries.sh --run     # execute all four
```

All scripts default to namespace `public/default` — override with `SL_NAMESPACE=<tenant>/<ns>`.

---

## 6. Storage layout on the disk

With `SL_STORAGE_DIR=/mnt/nvme` the demo uses:

| Path | Holds |
|---|---|
| `/mnt/nvme/zk` | ZooKeeper data (tiny — StreamLake keeps only pointers here) |
| `/mnt/nvme/bk/journal` | BookKeeper write‑ahead log |
| `/mnt/nvme/bk/ledgers` | BookKeeper ledgers = **the table data** + StreamLake page‑index/segment/catalog ledgers |
| `/mnt/nvme/streamlake` | reserved for the future query‑executor tier |

Rough sizing: ~`Person_GB + Employee_GB` of data ledgers (RF 1, single bookie) plus a small % for
StreamLake metadata ledgers.

---

## 7. Ingestion summary (what `sl-info.sh` shows)

Exact **rows** are echoed by the load generator as it runs:
```
DONE Person: wrote 12,500,000,000 rows in 5400s (2314814 rows/s); storage=500.10 GB; nextStartId=12500000000
```

The **on‑storage layout** per table (`pulsar-admin streamlake info public/default Person`):
```
StreamLake table: persistent://public/default/Person
  data ledgers      : 10,240 total  (10,238 segmented, 2 not-yet-segmented)
  page-index ledgers: 11  ids=[5, 88, 171, ...]
  segment ledgers   : 1   ids=[9]
  catalog ledger    : 4
  data pages        : 12,500,000 (data-ledger entries; rows = pages x rowsPerPage)
  event-time range  : 1,700,000,000,000 .. 1,700,005,400,000 (epoch ms)
```
- **data / page‑index / segment / catalog ledgers** — the full ledger inventory of each tier.
- **data pages** = data‑ledger entries; **rows = pages × rowsPerPage** (`rowsPerPage`=1000 by default).
- **event‑time range** = the publish‑time window the data covers (this is what date pruning uses).

Also inspect the raw data‑ledger chain:
```bash
$PULSAR_HOME/bin/pulsar-admin topics stats-internal persistent://public/default/Person
```

---

## 8. The queries (via `pulsar-admin streamlake query`)

Table names are the topics in the namespace. Output is a table + a **stats footer** (rows read, pages
scanned/kept/pruned, bytes read, peak buffer, elapsed). Add `--json` for raw NDJSON. All four below are
**bounded** so they return fast even over multi‑TB tables — widen or drop the `personId` ranges to scan
more.

```bash
Q="$PULSAR_HOME/bin/pulsar-admin streamlake query public/default"

# 1) INNER JOIN — Person ⋈ Employee ON personId (over a pruned personId slice)
$Q "SELECT * FROM Person p JOIN Employee e ON p.personId = e.personId \
    WHERE p.personId BETWEEN 0 AND 100000 AND p.age BETWEEN 30 AND 40 AND e.salary >= 40000"

# 2) GROUP BY — age -> COUNT/MIN/MAX (scans a slice, returns 50 rows)
$Q "SELECT age, COUNT(*), MIN(personId), MAX(personId) FROM Person \
    WHERE personId BETWEEN 0 AND 1000000 GROUP BY age"

# 3) ORDER BY — top-20 highest salaries (bounded top-K external sort)
$Q "SELECT personId, salary FROM Employee \
    WHERE personId BETWEEN 0 AND 1000000 ORDER BY salary DESC LIMIT 20"

# 4) SCAN a key range to validate the data (page-pruned to just that slice)
$Q "SELECT personId, name, age FROM Person WHERE personId BETWEEN 0 AND 20"
```

**Full‑table variants** (heavy scan over the whole table, tiny result — good stress demo):
```bash
$Q "SELECT age, COUNT(*) FROM Person GROUP BY age"
$Q "SELECT personId, salary FROM Employee ORDER BY salary DESC LIMIT 20"
```

**Checkable data model** (so answers are verifiable): row `i` has `Person.age = 20 + (i % 50)` and
`Employee.salary = 30000 + (i % 50) * 1000`, with `personId = i` shared by both tables. So the join
`age ∈ [30,40] AND salary ≥ 40000` qualifies exactly `i % 50 ∈ [10, 20]` (11 of every 50). For the join
query above (`personId 0..100000`): **≈ 22,000 rows**.

> **Date range:** StreamLake prunes by **event (publish) time** automatically — the covered window is the
> `event-time range` from `streamlake info`. Range predicates on **indexed columns** (e.g. `personId
> BETWEEN …`) prune to just the pages in that slice, which is the "scan a range to validate" path. (An
> explicit `--from/--to` epoch‑ms flag on `query` is a planned follow‑up.)

---

## 9. Long ingestion: run detached, interrupt, resume, scale up

**Run it so an ssh drop can't kill it** (a 1 TB load runs for hours):
```bash
# inside tmux (recommended) — survives disconnects. Detach: Ctrl-b then d ; reattach: tmux attach -t sl
tmux new -s sl
streamlake-demo/scripts/sl-ingest.sh
# ...or nohup:
nohup streamlake-demo/scripts/sl-ingest.sh > "$PULSAR_HOME/logs/ingest.out" 2>&1 &
tail -f "$PULSAR_HOME/logs/ingest.out"
```

**If ingestion was interrupted and you re-run it** (e.g. ssh dropped):
- You do **not** need to clean anything — data already written is durable in BookKeeper; re-running only
  adds more, nothing is corrupted.
- A table that already reached its size target is **skipped** (the generator checks storage first), so a
  completed table is never re-written.
- A table that stopped **part-way** resumes from `--start-id` (default 0), so re-running with defaults
  appends rows with **duplicate personIds** until the size target — fine for a rough demo, but it skews
  the exact join/group counts. To resume **cleanly**, pass the id it reached (`sl-ingest` prints
  `nextStartId=…` on a clean finish; if it was killed, estimate ≈ data pages × rowsPerPage from
  `sl-info.sh`), e.g. `SL_EMP_START=8000000000 streamlake-demo/scripts/sl-ingest.sh`. Or wipe and start
  fresh (§10).

**Scale up** (append more) — raise the target and start where it ended:
```bash
SL_PERSON_GB=1000 SL_EMP_GB=1000 \
  SL_PERSON_START=12500000000 SL_EMP_START=12500000000 \
  streamlake-demo/scripts/sl-ingest.sh
streamlake-demo/scripts/sl-info.sh
```
Change per‑page density with `SL_ROWS_PER_PAGE` (default 1000) and RF/rollovers at register time
(`SL_RF`, `SL_PI_MAX`, `SL_SEG_MAX`) — see §11.

---

## 10. Stop the broker / reset / troubleshoot

**Stop the server (broker + bookie + zk):**
```bash
streamlake-demo/scripts/sl-start.sh stop         # graceful (uses the pid file)
streamlake-demo/scripts/sl-start.sh status       # healthy / not ready
# manual equivalent:  kill "$(cat "$PULSAR_HOME/data/standalone.pid")"
```
Stopping **keeps all data on disk** — restart with `sl-start.sh start` and the tables are still there
(no re-ingest needed). Ingestion running in `tmux`/`nohup` is a separate process; stop it from its
session or `kill` its `java` PID.

**Full reset (start over with an empty cluster):**
```bash
streamlake-demo/scripts/sl-start.sh stop
rm -rf "$SL_STORAGE_DIR"/{zk,bk} "$PULSAR_HOME/data"    # e.g. SL_STORAGE_DIR=/grid/x/dfs-data/tmp/test
SL_STORAGE_DIR="$SL_STORAGE_DIR" SL_PERSON_GB=500 SL_EMP_GB=500 streamlake-demo/scripts/sl-demo.sh
```
- **`LedgerNotExistException` on a query:** data ledgers were trimmed. `sl-register.sh` sets infinite
  retention; if you registered manually, run
  `pulsar-admin namespaces set-retention public/default --size -1 --time -1`.
- **Query says "not a loaded StreamLake table":** the topic isn't registered on the owning broker — run
  `sl-register.sh` (or `streamlake register …`) first.
- **Logs:** `$PULSAR_HOME/logs/standalone.out`.

---

## 11. Command + environment reference

**`pulsar-admin streamlake` subcommands**

| Command | What it does |
|---|---|
| `register <ns> <table> --schema "name:TYPE[:noidx],…"` | make a topic a StreamLake table. Flags: `--rf` (replication, 1 for one bookie), `--page-index-max-entries`, `--segment-max-entries`, `--max-cardinality`, `--bloom-fpp`, `--async-build`. TYPE ∈ INT32,INT64,DOUBLE,BOOLEAN,STRING,BYTES; columns indexed unless `:noidx`. |
| `info <ns> <table> [--json]` | ledger inventory + data pages + event‑time range. |
| `query <ns> "<SQL>" [--json]` | run a scan / inner join / group‑by / order‑by; prints a table + stats footer. |

Register examples:
```bash
$PULSAR_HOME/bin/pulsar-admin streamlake register public/default Person \
  --schema "personId:INT64,name:STRING,age:INT32" --rf 1
$PULSAR_HOME/bin/pulsar-admin streamlake register public/default Employee \
  --schema "empId:INT64,personId:INT64,salary:INT64" --rf 1
```

**Script environment variables**

| Var | Default | Used by | Meaning |
|---|---|---|---|
| `PULSAR_HOME` | dist root (auto) | all | unpacked distribution dir |
| `SL_STORAGE_DIR` | `/mnt/nvme` | sl-demo | base disk for zk + bookie data |
| `SL_PERSON_GB` / `SL_EMP_GB` | `500` | sl-demo, sl-ingest | target on‑disk table size (GB) |
| `SL_NAMESPACE` | `public/default` | all | `<tenant>/<namespace>` |
| `SL_JOURNAL_DIR` / `SL_LEDGER_DIRS` / `SL_QUERY_LOCAL_DIR` | under `SL_STORAGE_DIR` | sl-configure | bookie journal / ledgers / query scratch |
| `SL_ZK_DIR` / `SL_BK_DIR` | `$PULSAR_HOME/data/*` | sl-start | zk + bookie data dirs |
| `SL_RF` | `1` | sl-register | replication factor (page‑index + segment ledgers) |
| `SL_PI_MAX` / `SL_SEG_MAX` | `1000000` / `200000` | sl-register | page‑index / segment ledger rollover |
| `SL_ROWS_PER_PAGE` | `1000` | sl-ingest | rows per columnar page |
| `SL_PERSON_START` / `SL_EMP_START` | `0` | sl-ingest | starting id (for append re‑runs) |
| `SL_SERVICE_URL` / `SL_ADMIN_URL` | `pulsar://localhost:6650` / `http://localhost:8080` | sl-ingest | broker endpoints |

**Load generator directly** (`streamlake-demo/ingest/StreamLakeIngest.java`, compiled by `sl-ingest.sh`).
Note the JDK 17+ (`$JAVA_HOME/bin/java`) and the Arrow `--add-opens` flags — `sl-ingest.sh` adds these for
you; only needed if you run it by hand:
```bash
"$JAVA_HOME/bin/java" -Xmx2g \
  -Dio.netty.tryReflectionSetAccessible=true \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -cp "$PULSAR_HOME/lib/*:streamlake-demo/ingest/out" StreamLakeIngest \
  --service-url pulsar://localhost:6650 --admin-url http://localhost:8080 \
  --tenant public --namespace default --table Person \
  --target-gb 500 --rows-per-page 1000 --start-id 0
#   ... or --rows N   for an exact row count instead of a size target.
```

### Host gotchas (now auto-handled by the scripts; here for reference / manual runs)
- **`javac` is Java 8** (`class file has wrong version 61.0, should be 52.0`): the client jars are Java 17
  bytecode. `sl-ingest.sh` **auto-detects a JDK 17+** (prefers `$JAVA_HOME`, else the JDK behind the
  `java` on PATH). Manual override if detection fails: `export JAVA_HOME=/path/to/jdk17`.
- **Arrow `UnsupportedOperationException: … DirectByteBuffer … not available`**: JDK 17 needs the Arrow
  `--add-opens`. `sl-ingest.sh` **sets them automatically**; by hand, `export
  JDK_JAVA_OPTIONS="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"`.
- **macOS `._*` files** (`error in opening zip file`): the scripts run `find "$PULSAR_HOME" -name '._*'
  -delete` automatically.

---

## 12. Every script — what it means, how to run it, what to expect

All scripts live in `streamlake-demo/scripts/`. Order of use and dependencies:

| Script | Host | Needs server running? | Needs tables registered? |
|---|---|---|---|
| `sl-package.sh` | build | — | — |
| `sl-configure.sh` | remote | no (edits conf) | — |
| `sl-start.sh` | remote | it starts it | — |
| `sl-register.sh` | remote | **yes** | it registers them |
| `sl-ingest.sh` | remote | **yes** | **yes** |
| `sl-info.sh` | remote | **yes** | **yes** |
| `sl-queries.sh` | remote | **yes** | **yes** |
| `sl-demo.sh` | remote | runs all of the above in order | — |

`sl-demo.sh` runs the whole chain; the individual scripts are for running or re‑running a single phase.

### `sl-package.sh` — build the deployable tar (build host)
- **Means:** compiles the fork into a Pulsar **server distribution** and bundles `streamLake/demo/`
  (docs + scripts + ingest) into it.
- **Run** (from the repo root `pulsar/pulsar`):
  ```bash
  streamLake/demo/scripts/sl-package.sh
  ```
- **Expect:** a Gradle build (minutes on first run) printing `[1/4]…[4/4]`, then:
  ```
  Done: /…/pulsar/pulsar/streamlake-demo.tar.gz
  Ship + unpack + run the whole demo on the host:
    scp streamlake-demo.tar.gz user@HOST:/opt/
    …
  ```
  Produces `streamlake-demo.tar.gz` at the repo root. No server is started.

### `sl-configure.sh` — point storage at your disk (remote host)
- **Means:** idempotently writes bookie storage keys (`journalDirectory`, `ledgerDirectories`,
  `ledgerStorageClass=DbLedgerStorage`) and broker keys (`systemTopicEnabled`,
  `topicLevelPoliciesEnabled`, `brokerDeleteInactiveTopicsEnabled=false`) into `conf/standalone.conf`
  and `conf/bookkeeper.conf`. **No manual file edits, no server start.**
- **Run:**
  ```bash
  export PULSAR_HOME=/opt/apache-pulsar-<ver>
  SL_JOURNAL_DIR=/mnt/nvme/bk/journal SL_LEDGER_DIRS=/mnt/nvme/bk/ledgers \
  SL_QUERY_LOCAL_DIR=/mnt/nvme/streamlake  streamlake-demo/scripts/sl-configure.sh
  ```
- **Expect:**
  ```
  Configured:
    journal (NVMe)   : /mnt/nvme/bk/journal
    ledgers (HDD/TB) : /mnt/nvme/bk/ledgers
    query local dir  : /mnt/nvme/streamlake
    standalone.conf  : …/conf/standalone.conf
    bookkeeper.conf  : …/conf/bookkeeper.conf
  ```

### `sl-start.sh` — start / stop / status the server (remote host)
- **Means:** runs `bin/pulsar standalone` (ZooKeeper + bookie + broker) in the background, with zk +
  bookie data on your disk (`SL_ZK_DIR`, `SL_BK_DIR`).
- **Run:**
  ```bash
  SL_ZK_DIR=/mnt/nvme/zk SL_BK_DIR=/mnt/nvme/bk streamlake-demo/scripts/sl-start.sh start
  streamlake-demo/scripts/sl-start.sh status      # -> healthy | not ready
  streamlake-demo/scripts/sl-start.sh stop
  ```
- **Expect (start):**
  ```
  Starting Pulsar standalone (zk + bookie + broker)…
  PID 12345; logs: …/logs/standalone.out
    zk data : /mnt/nvme/zk
    bk data : /mnt/nvme/bk
  Wait for readiness:  until …/bin/pulsar-admin brokers healthcheck; do sleep 2; done
  ```
  Then poll `pulsar-admin brokers healthcheck` until it succeeds (~10–30 s).

### `sl-register.sh` — register Person + Employee (remote host)
- **Means:** sets infinite retention on the namespace, creates the two topics if absent, and applies the
  StreamLake table policy (schema + tuning) via `pulsar-admin streamlake register`. **Server must be up.**
- **Run:**
  ```bash
  streamlake-demo/scripts/sl-register.sh
  # tune: SL_NAMESPACE (public/default), SL_RF (1), SL_PI_MAX (1000000), SL_SEG_MAX (200000)
  ```
- **Expect:**
  ```
  ==> setting infinite retention on public/default …
  ==> creating topic persistent://public/default/Person …
  ==> registering Person (personId:INT64, name:STRING, age:INT32)…
  Registered StreamLake table public/default/Person with 3 columns (rf=1).
  ==> registering Employee (empId:INT64, personId:INT64, salary:INT64)…
  Registered StreamLake table public/default/Employee with 3 columns (rf=1).
  Registered Person + Employee in namespace public/default.
  ```

### `sl-ingest.sh` — load the data (remote host)
- **Means:** compiles `ingest/StreamLakeIngest.java` against `$PULSAR_HOME/lib/*`, then runs it to load
  Person then Employee up to their target sizes (polls topic storage to hit the GB target).
  **Server up + tables registered.** Long‑running for large sizes; safe to leave in a `screen`/`tmux`.
- **Run:**
  ```bash
  SL_PERSON_GB=500 SL_EMP_GB=500 streamlake-demo/scripts/sl-ingest.sh
  # tune: SL_ROWS_PER_PAGE (1000), SL_PERSON_START / SL_EMP_START (0, for append re-runs)
  ```
- **Expect:**
  ```
  ==> compiling StreamLakeIngest against …/lib…
  ==> ingesting Person to ~500GB (startId=0)…
    1,000,000 rows  240000 rows/s  storage=0.04 GB
    2,000,000 rows  242000 rows/s  storage=0.08 GB
    …
  DONE Person: wrote 12,500,000,000 rows in 5400s (…rows/s); storage=500.10 GB; nextStartId=12500000000
  ==> ingesting Employee to ~500GB (startId=0)…
    …
  Ingestion complete.
  ```

### `sl-info.sh` — inspect the storage layout (remote host)
- **Means:** prints `pulsar-admin streamlake info` for both tables (ledger inventory, data pages,
  event‑time range) plus a few lines of the raw data‑ledger chain.
- **Run:** `streamlake-demo/scripts/sl-info.sh`
- **Expect:** for `Person` and `Employee`, the `StreamLake table: …` block shown in §7, followed by
  `"ledgerId"/"entries"/"size"` lines from `topics stats-internal`.

### `sl-queries.sh` — the demo queries (remote host)
- **Means:** the three demo queries (inner join, group‑by, order‑by) + a validation scan.
  Default **prints** the copy‑paste commands; `--run` **executes** all four.
- **Run:**
  ```bash
  streamlake-demo/scripts/sl-queries.sh          # print
  streamlake-demo/scripts/sl-queries.sh --run     # execute all four
  ```
- **Expect (--run):** each query prints a result **table** followed by its **stats footer** (rows
  returned/read, pages scanned/kept/pruned, bytes read, peak buffer, elapsed).

### `sl-demo.sh` — the one‑command orchestrator (remote host)
- **Means:** runs `sl-configure → sl-start → (wait healthy) → sl-register → sl-ingest → sl-info →
  sl-queries` after prompting for the disk + the two table sizes.
- **Run:**
  ```bash
  cd /opt/apache-pulsar-<ver>
  streamlake-demo/scripts/sl-demo.sh                                   # interactive
  SL_STORAGE_DIR=/mnt/nvme SL_PERSON_GB=500 SL_EMP_GB=500 streamlake-demo/scripts/sl-demo.sh  # non-interactive
  ```
- **Expect:** the prompts (§4), then the banners of each phase, ending with the **ingestion summary** and
  the **queries** to copy‑paste. Stop later with `sl-start.sh stop`.

