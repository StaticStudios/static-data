# Current performance baseline

This document records measurements of the current Static Data implementation. Results are machine-specific and should be compared on the same idle host, JVM, benchmark parameters, and commit.

## Read throughput

`ReadThroughputBenchmark` reports explicit operations per second for production read paths. The following results used Java 21, eight reader threads, and a retained, prewarmed 100-player set. Each player has one settings reference and eight friends.

A compound player read performs a player instance lookup, resolves the settings reference and underlying settings instance, then reads the settings priority and player name.

| Read operation | Throughput |
| --- | ---: |
| Instance-cache lookup | 25.94 ± 6.07 million ops/s |
| Player lookup + persistent value | 12.79 ± 1.11 million ops/s |
| Player lookup + settings reference | 12.35 ± 1.65 million ops/s |
| Compound player read | 5.72 ± 0.46 million ops/s |
| Friend collection | 0.497 ± 0.040 million ops/s |
| Complete 100-player scan | 3,777 ± 258 scans/s |

Working-set retention is parameterized. With eight readers selecting across 1,000 players, compound throughput was 4.33 ± 0.77 million reads/s when the complete set was retained and prewarmed. It was 3.94 ± 0.50 million reads/s when only the 100-player tick set was retained and the other 900 entries could be reclaimed between iterations.

## End-to-end player scan

`StaticDataBenchmark` uses the current DataManager, H2 mirror, PostgreSQL 16.2, and Redis 7.4.1. It creates 32 players with settings and friends, then runs one simulated Minecraft server thread alongside four cache-reader threads.

| Operation | Result |
| --- | ---: |
| Hot `DataManager.getInstance()` hit | 0.06 ± 0.02 µs/op |
| Complete 32-player scan under asynchronous load | 538.22 ± 101.91 µs/op |

The modeled scan consumes about 0.54 ms, or 1.1% of a 50 ms tick budget, on the measurement host. It is a regression workload, not a production TPS prediction.

## Cross-container load

`CrossContainerLoadBenchmark` uses 100 players with eight friends and one settings reference per player. Peer sessions update PostgreSQL rows across disjoint partitions of the configured write set. Each committed update traverses the PostgreSQL trigger, `NOTIFY`, the full Static Data listener, H2 application, cache invalidation, and subsequent reads. Seven additional listener connections model notification fan-out.

The mixed-load runs use 50 writes/s per peer session:

| Remote load | Tick mean | Tick p95 | Tick p99 | Async-read p50 | Async-read p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Local-only control | 1.65 ms | 2.30 ms | 3.21 ms | 1.4 µs | 2.8 µs |
| 1 peer writer, 50 writes/s | 2.24 ms | 3.33 ms | 4.80 ms | 1.6 µs | 6.5 µs |
| 8 peer writers, 400 writes/s | 2.42 ms | 3.49 ms | 4.66 ms | 1.7 µs | 4.3 µs |

At 400 remote writes/s, the modeled tick p99 consumes about 9.3% of a 50 ms tick budget on this host.

The isolated four-peer measurements are:

| Distributed operation | Mean | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL write/trigger/commit round trip | 1.13 ms | 1.07 ms | 1.72 ms | 2.22 ms |
| Update request to visibility in the listening cache | 1.34 ms | 1.30 ms | 1.89 ms | 2.33 ms |

These cover the distributed pipeline end to end. They do not isolate network delivery, listener work, H2 application, and cache invalidation into separate timings.

## Controlled high-read load

The controlled workload uses eight reader threads sharing one aggregate rate limiter. In parallel, one thread continuously scans all 100 players, eight peer sessions target a combined 400 writes/s, and eight PostgreSQL listeners receive invalidations.

| Compound-read target | Achieved | Tick mean | Tick p95 | Tick p99 |
| ---: | ---: | ---: | ---: | ---: |
| 100,000/s | 98,568/s | 2.09 ms | 2.88 ms | 3.56 ms |
| 250,000/s | 246,294/s | 2.17 ms | 3.13 ms | 4.33 ms |
| 500,000/s | 496,101/s | 2.15 ms | 3.01 ms | 4.17 ms |

The paced writers achieved about 400 updates/s and the seven notification-only peers observed about 2,800 callbacks/s, confirming the expected notification fan-out. No throughput cliff appeared by 500,000 compound reads/s on the measurement host.

The JMH score for a controlled reader includes its wait for the next permit. Use the emitted achieved-rate line for controlled workloads and `ReadThroughputBenchmark` for saturation capacity.

## Current slow paths

1. `PersistentManyToManyCollectionImpl.getIds()` constructs SQL and executes an H2 join query for every collection read. This is the clearest measured read bottleneck. A membership cache needs dependency or generation invalidation that also handles remotely inserted join rows.
2. `ReferenceImpl.getReferencedColumnValuePairs()` creates query inputs and identifier objects on cached reference reads. A cached per-reference lookup key could reduce allocation if it is invalidated when holder ID or linking columns change.
3. A complete player scan repeatedly resolves relationships and collection members. Consumers that read the same projection several times during one tick may benefit from a server-layer per-tick snapshot.

The benchmarks intentionally retain these production paths. Add a focused benchmark before optimizing one, then compare measurements on the same machine and commit range.

## Scope and limitations

The container-backed results use local Docker networking. Notification-only peers do not instantiate complete DataManager and H2 stacks. The suite does not model other plugins, Minecraft engine work, WAN latency, or database hosts under unrelated load. The continuously repeated tick scan is a contention stress workload rather than a 20 Hz scheduler.
