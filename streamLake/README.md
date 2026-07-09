# StreamLake documentation

StreamLake turns a Pulsar topic into a **fresh‑data, columnar, predicate‑prunable table store** you
can prune, scan, filter, top‑K and **inner‑join with SQL**, while the pub/sub write path stays a
dumb, low‑latency pipe. This directory collects all StreamLake documentation.

## Current / authoritative

| Doc | What it covers |
|---|---|
| [`DESIGN.md`](DESIGN.md) | **The authoritative end‑to‑end design & implementation.** Write path (client Arrow batching + pruning stats → dumb‑pipe broker → page‑index ledger durability barrier), the `/streamlake` znode metadata tier, async column‑oriented segment build, hierarchical pruning (date → segment → page), late materialization, the off‑heap hash **inner join**, the Calcite SQL frontend, byte‑level formats, and the configuration reference (§9). |
| [`BENCHMARK.md`](BENCHMARK.md) | **Runnable inner‑join benchmark + cost model.** Measured pruning / IO amplification / off‑heap spill on a 10 GB / 2‑day / 5‑min‑rollover layout, the tooling & source paths for future testing (§7.1), a fair methodology vs Kafka + Spark + S3 + Iceberg, and a transparent list‑price TCO model (HDD vs NVMe ledger devices). |

Start with `DESIGN.md`, then `BENCHMARK.md`.

## Legacy / superseded (kept for history)

These describe the **earlier prototype** architecture (broker‑side batching, a **bookie‑resident**
min/max + bloom page index, and a broadcast‑hash join with runtime semi‑join push‑down). That
approach was **redesigned** — the page index moved **off the bookie** into the `/streamlake` metadata
tier, encoding moved to the client, and segments were introduced. Read these only for background; for
the code as it exists today, use `DESIGN.md`.

| Doc | What it covered (prototype) |
|---|---|
| [`STREAMLAKE.md`](STREAMLAKE.md) | Prototype end‑to‑end flow, design proposal, and the original bookie‑page‑index pruning (date partition → bookie column ranges → row filter). |
| [`STREAMLAKE_JOIN.md`](STREAMLAKE_JOIN.md) | Prototype bloom‑filter page index and the broadcast‑hash inner join with runtime semi‑join push‑down. |

## Related source (repo‑root‑relative)

- Broker query tier / metadata stack: `pulsar-broker/src/main/java/org/apache/pulsar/broker/service/streaminglake/`
- Client encoding / join tables: `pulsar-client/src/main/java/org/apache/pulsar/client/streaminglake/`
- Benchmark harness: `pulsar-broker/src/test/java/org/apache/pulsar/broker/service/streaminglake/StreamLakeJoinBenchmark.java`
