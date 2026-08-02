# StreamLake — Query Execution Walkthrough (10K data ledgers)

Grounded in the current code (Phase A on‑demand loading + Phase B entry‑based rollover + Phase C
exact‑set recheck). Numbers use these **assumptions** (state them when quoting):

- **10,000 data ledgers** for a topic (e.g. `Person`).
- **50,000 pages (entries) per data ledger** (1 page ≈ 1 MiB ≈ 1,000 columnar rows) → **500M pages**,
  ~**500B rows**.
- **4 indexed columns** (e.g. `personId, name, age, createTime`).
- Config defaults: `pageIndexMaxEntriesPerLedger = 1,000,000`, `segmentMaxEntriesPerLedger = 200,000`,
  `segmentCacheMaxEntries = 512`.

## 1. How many ledgers of each kind?

| Ledger kind | Count | Why |
|---|---|---|
| **data ledgers** | 10,000 | given |
| **page‑index ledgers** | **500** | 1 footer per page → 50K footers/data‑ledger; one page‑index ledger holds 1,000,000 footers ⇒ **20 whole data ledgers per page‑index ledger** (rolled at a data‑ledger boundary, so each data ledger's footers are contiguous) ⇒ 10,000 / 20 = **500** |
| **segment ledgers** | **1** | 1 segment = 1 `'D'` + 4 `'C'` = 5 entries/data‑ledger; one segment ledger holds 200,000 entries ⇒ 40,000 data‑ledger segments ⇒ 10,000×5 = 50,000 entries ⇒ fits in **1** segment ledger |
| **catalog ledger** | **1** | one manifest entry (89 B) per data ledger ⇒ 10,000×89 ≈ **0.85 MB**, one BK ledger |

## 2. What ZK stores (the `/streamlake/<tenant>/<ns>/<topic>` znode)

Only tiny **pointers** — a table of contents, no per‑ledger data:

```
version(1) | flags(1) |
  nSeg=1    | segIds   = [ S1 ]                         (1 segment ledger)
  nPi=500   | piIds    = [ P1, P2, …, P500 ]            (500 page-index ledgers, 8 B each ≈ 4 KB)
  catalogId = C1                                        (the catalog ledger)
```

Total znode ≈ **~4 KB** (dominated by the 500 page‑index ledger ids). It does **not** grow with pages
or rows — only with page‑index ledger count.

## 3. What the catalog (manifest) holds — the only resident tier

Replayed once on open into memory (~0.85 MB). One entry per data ledger:

```
dataLedgerId | createTs | minEventTime | maxEventTime | rowCount | state |
  segmentLedgerId  | segmentStartEntry  | segmentEndEntry     |   ← where this ledger's segment lives
  pageIndexLedgerId| pageIndexStartEntry| pageIndexEndEntry            ← its exact page-index range
```

Example rows (event‑time ordered ingest ⇒ ranges roughly increasing):

```
DL 5001 | … | 2026-03-01T00:00 | 2026-03-01T02:47 | 50,000,000 | SEGMENTED | S1, e=250,000 .. e=250,004 | P251, e=0 .. e=49,999
DL 5002 | … | 2026-03-01T02:47 | 2026-03-01T05:34 | 50,000,000 | SEGMENTED | S1, e=250,005 .. e=250,009 | P251, e=50,000 .. e=99,999
…
```

## 4. Query: `SELECT * FROM Person WHERE createTime IN [T0,T1) AND name = 'John'`

### Step A — date prune (in‑memory catalog, reads **no** segments)
`catalog.candidateLedgers(T0, T1)` scans the resident manifest and keeps data ledgers whose
`[minEventTime,maxEventTime]` intersects the window. Say `[T0,T1)` is a 12‑hour window → ~**100** of the
10,000 data ledgers survive (each covers ~2.8 h). **9,900 ledgers are never touched**, and **no segment
or page‑index ledger is read yet** — this is a pure in‑RAM filter over ~100 candidate ids.

### Step B — load each candidate's segment on demand (bounded LRU)
For each of the ~100 candidates, `segmentStore.load(dataLedgerId, info.segmentLedgerId,
segmentStart, segmentEnd)` seeks **exactly that segment's 5 entries** in `S1` (cached in the
`segmentCacheMaxEntries=512` LRU). We load ~100 small segments — **not** all 10,000, and never the whole
segment ledger.

### Step C — page prune on `name = 'John'` (from the segment)
The `name` column segment is consulted per candidate ledger:

- **If `name` is per‑page** (low cardinality, array kept): `candidatePositions(name='John')` tests each
  page's **bloom** (built from that page's `set(N)`). A page survives only if its bloom *might* contain
  `'John'`. Of a ledger's 50,000 pages, typically a **handful** survive (bloom is precise).
- **If `name` collapsed** (high cardinality ⇒ one whole‑segment bloom): the segment gives a coarse
  "maybe" for the whole ledger. **Phase C** then kicks in: `pageIndex.readRange(info.pageIndexLedgerId,
  pageIndexStart, pageIndexEnd)` reads that ledger's **50,000 exact footers** from `P<n>` and, per
  surviving page, checks the exact `set(N)` — dropping every page whose set does **not** contain
  `'John'` (killing the bloom's false positives). This is one targeted ~13 MB range read per candidate
  ledger, replacing reading ~50 GB of data.

Net: from 500M pages → ~100 candidate ledgers → a few surviving **pages** per ledger (say ~5 each) →
**~500 pages** to actually read.

### Step D — read only surviving pages + exact row filter
For each surviving `(dataLedgerId, pageEntryId)`, the executor reads the **1 MiB Arrow page**, decodes,
and applies `predicate.matchesRow(row)` — the exact `name == 'John'` filter (removes any residual
false positives). With Phase D this reads the ~500 pages with `queryReadConcurrency` (default 16) in
parallel.

**Result:** ~500 pages read out of 500M (**~0.0001%**), no shuffle, resident memory ≈ catalog (0.85 MB)
+ ~100 cached segments + the surviving rows.

## 5. Inner join example: `Person ⋈ Employee ON personId`

```sql
SELECT p.personId, p.name, e.salary
FROM Person p INNER JOIN Employee e ON p.personId = e.personId
WHERE p.createTime IN [T0,T1) AND p.age BETWEEN 30 AND 40
  AND e.startDate IN [T0,T1) AND e.salary >= 300000
```

1. **Build side (Person)** — Steps A–D above with `age BETWEEN 30 AND 40` (numeric per‑page min/max
   prune, no bloom needed) → read only surviving Person pages → for each row, `join.addBuildRow` into
   the (on‑heap or off‑heap `SpillingJoinTable`) hash table keyed by `personId`.
2. **Probe side (Employee)** — Steps A–D with `salary >= 300000` → read only surviving Employee pages,
   but **late‑materialized**: read only the `personId` cell per row, `join.matches(personId)`; only on a
   hash hit materialize the full Employee row and emit `concat(employeeRow, personRow)`.

Both sides prune independently to a tiny page set; the join runs **broker‑local** (build spills to
local disk if large) with **no distributed shuffle**.
