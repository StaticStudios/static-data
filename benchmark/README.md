# Static Data benchmarks

The benchmark module contains three layers:

- `StaticDataBenchmark` is an integration benchmark backed by Testcontainers PostgreSQL and Redis plus Static Data's H2 cache. Its grouped workload models one Minecraft server thread resolving 32 players, settings references, and friend collections while four asynchronous workers resolve cached players.
- `ReadThroughputBenchmark` reports explicit operations/second for individual production read paths and a complete configurable player scan. It defaults to eight reader threads, 100 retained players, and a fully warmed working set.
- `CrossContainerLoadBenchmark` models one listening Static Data container and peer containers connected to the same PostgreSQL database. Peer writers use persistent database sessions with distinct application names, so writes traverse the real PostgreSQL trigger, `NOTIFY`, Static Data listener, H2 mirror update, cache invalidation, and subsequent local-read path. Additional notification-only listeners model PostgreSQL fan-out without incorrectly sharing a single H2 mirror between simulated containers.

`CrossContainerLoadBenchmark` supplies these scenarios:

- `localOnly`: one 100-player tick scan plus four local asynchronous readers, used as the control.
- `readHeavyCrossContainer`: the same readers plus one peer writer, paced to 50 writes/s by default.
- `writeHeavyCrossContainer`: the same readers plus eight peer writers, paced to a combined 400 writes/s by default.
- `controlledReadLoad`: one tick scanner plus eight readers sharing an explicit aggregate target of 250,000 compound reads/s by default.
- `controlledMixedLoad`: the controlled readers plus eight peer writers (400 writes/s total by default) and the configured notification listeners.
- `remoteWriteRoundTrip`: four unpaced peer sessions measuring PostgreSQL update/trigger/commit latency.
- `remoteUpdatePropagation`: four peer sessions measuring the complete update-request-to-local-cache-visibility latency.

One compound read performs a player instance lookup, resolves its settings reference (including the settings instance lookup), and reads the settings priority and player name. Thus, 250,000 compound reads/s represents roughly one million public API-level lookup/value operations per second.

The controlled-reader and mixed-load writer scores include intentional pacing and should not be interpreted as operation latency. The load-rate lines emitted after every iteration are the authoritative achieved read/write rates and also report notification fan-out plus Static Data's rolling H2 counters. Use `ReadThroughputBenchmark` for maximum read throughput and `remoteWriteRoundTrip` for database-write latency. The tick loop runs continuously rather than at 20 Hz, making it a contention stress test rather than a literal server scheduler.

Docker must be running for the benchmark suite.

Run the end-to-end Minecraft workload:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*StaticDataBenchmark.*'
```

Report maximum throughput for every production read path:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*ReadThroughputBenchmark.*'
```

Run the cross-container workloads:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*CrossContainerLoadBenchmark.*'
```

Run only the write-heavy scenario:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*CrossContainerLoadBenchmark.writeHeavy.*'
```

Run a controlled 100k/250k/500k compound-read matrix while eight peers write at a combined 400 writes/s:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*CrossContainerLoadBenchmark.controlledMixed.*' "-PjmhParams=targetReadsPerSecond=100000,250000,500000"
```

The container-backed states default to eight PostgreSQL notification listeners, 100 database players, a 100-player hot set, 100 players scanned per tick operation, eight friends per player, 50 writes/s per peer writer, peer writes partitioned across 100 players, and a fully retained/prewarmed read set. Each peer owns a disjoint slice so propagation tests cannot overwrite a value before its writer observes it. Override JMH parameters without editing source:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*CrossContainerLoadBenchmark.writeHeavy.*' "-PjmhParams=listenerCount=16;playerCount=1000;hotPlayerCount=500;playersPerTick=250;remoteWritePlayerCount=500;remoteWritesPerSecond=100"
```

Multiple values create a parameter matrix, for example `"-PjmhParams=listenerCount=1,4,8,16;remoteWritesPerSecond=10,50,100"`. `hotPlayerCount`, `playersPerTick`, and `remoteWritePlayerCount` must not exceed `playerCount`; `remoteWritePlayerCount` must also be at least the scenario's peer-writer count (eight for the heavy groups). A read or write rate of zero disables its pacing and runs it at saturation.

To expose weak-cache misses and rehydration instead of measuring only a permanently hot online-player set, use a larger read set and disable full retention/prewarming:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*ReadThroughputBenchmark.compoundPlayerRead.*' "-PjmhParams=playerCount=1000;hotPlayerCount=1000;playersPerTick=100;warmReadWorkingSet=true,false"
```

Build a reader scaling curve by running the same command with `-PjmhThreads=1`, `4`, `8`, `16`, and `32`. The following optional project properties let CI or a local investigation make any scenario longer without editing annotations:

- `jmhThreads`: override the benchmark's reader-thread count.
- `jmhWarmupIterations`: number of warmup iterations.
- `jmhIterations`: number of measured iterations.
- `jmhTime`: duration of each iteration, such as `2s` or `60s`.
- `jmhForks`: independent benchmark JVM count.

For example, this is a five-minute measured soak at 500,000 compound reads/s plus 400 remote writes/s:

```powershell
.\gradlew.bat :benchmark:jmh -PjmhIncludes='.*CrossContainerLoadBenchmark.controlledMixed.*' "-PjmhParams=playerCount=1000;hotPlayerCount=1000;playersPerTick=250;targetReadsPerSecond=500000;warmReadWorkingSet=false" -PjmhWarmupIterations=2 -PjmhIterations=5 -PjmhTime=60s
```

Run every benchmark:

```powershell
.\gradlew.bat :benchmark:jmh
```

Machine-readable results are written to `benchmark/build/reports/jmh/results.json`; the complete console-style report is written to `benchmark/build/reports/jmh/human.txt`. Each invocation replaces these files, so copy them elsewhere when comparing separate runs.

See [`PERFORMANCE.md`](PERFORMANCE.md) for the current baseline, measured slow paths, and benchmark limitations.
