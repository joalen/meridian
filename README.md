# MapReduce Setup Instructions

Here we have many Apache Hadoop MapReduce jobs that run against the official [Apache Hadoop](https://github.com/apache/hadoop) docker compose file and spawns its nodes: NameNode, DataNode, ResourceManager, NodeManager.

## Pre-Requisites
- Docker + Docker Compose
- JDK 8
- Maven
- Git
- Hadoop cluster names namenode, datanode, resourcemanager, nodemanager

## General Steps
1. `git clone -b "docker-hadoop-3" https://github.com/apache/hadoop hadoop`
2. `cd hadoop`
3. Edit the docker-compose.yml file to add `container_name: namenode` to namenode service and `container_name: resourcemanager` to resourcemanager service
4. Edit the config file to point to /opt/hadoop
5. Startup Hadoop: `docker compose up -d`
6. Confirm containers: `docker ps`

Tips from running these on a laptop:
- Use `docker exec -it` when you want to see a job's console output. Without `-it` some terminals print nothing.
- If a job sits at "Running job" forever, check `yarn node -list`. A `LOST` node comes back with `docker restart hadoop-nodemanager-1`.
- After resetting the namenode, a datanode may refuse to start with "Incompatible clusterIDs". Recreate the datanode container.
- Two reducers is plenty for the data sizes here; more can overload a small Docker VM.

## Modules

Each module is its own Maven project with its own README that has the exact load/run/verify commands. Build any one of them from the repository root:

```sh
mvn -pl <Module> -am clean package     # produces <Module>/target/<Module>.jar
```

| Module | What it shows |
|--------|---------------|
| [TextMetrics](TextMetrics/) | Word counts, filtered counts and distinct-count patterns over a text corpus |
| [FieldIndexer](FieldIndexer/) | Chained jobs: inverted index, then the long word on the most lines |
| [GroupByAverage](GroupByAverage/) | Group-by with a combiner that carries `(sum,count)` so averages stay correct |
| [MapSideJoin](MapSideJoin/) | Join through the distributed cache, no shuffle for the join |
| [ReduceSideJoin](ReduceSideJoin/) | The same join as a reduce-side join with `MultipleInputs` |
| [DeltaChainResolver](DeltaChainResolver/) | Git pack delta chains: secondary sort, total-order partitioning, bounded top-K (all five stages done, see below) |

`DeltaChainResolver` must be listed in the root `pom.xml`:

```xml
<modules>
  <module>TextMetrics</module>
  <module>FieldIndexer</module>
  <module>GroupByAverage</module>
  <module>MapSideJoin</module>
  <module>ReduceSideJoin</module>
  <module>DeltaChainResolver</module>
</modules>
```

---

## Parallel Delta Chain Resolution via MapReduce (Git pack files)

**Status: all five stages implemented and verified on the cluster.** Module: [`DeltaChainResolver/`](DeltaChainResolver/) (its README has every command).

One of the interesting things about git is how it is able to send objects for when you do repository push/pull/rebase/etc. How it sends all of these items over the wire is through pack files.

Within a pack file are delta chains: each object may be stored not in full, but as a diff against a "base" object, which may itself be a diff against another base, and so on. Reconstructing any single object means walking its chain from the root outward, strictly in order. However, real pack files usually contain thousands of independent chains, all resolvable in parallel. That is a natural fit for MapReduce's secondary sort: the mapper tags each object with `(chainRootID, chainPosition)`, a custom partitioner and grouping comparator route a whole chain to one reducer, and Hadoop's shuffle sort delivers it in dependency order.

The data is real. The test repository is Lua (`28,004` objects in one repacked pack, `5,938` commits). On the cluster those objects form `6,472` chains: `21,532` deltas hanging off `6,472` full-content roots. The longest chain history spans 29 years.

### How the data gets to the cluster

Hadoop cannot read a `.pack` directly, so a few plain Java tools prepare the input. All of them run without a cluster.

```
git repack -a -d -f --depth=250 --window=250        one big pack, long chains
git verify-pack -v pack-*.idx > verify.txt          object, type, size, offset, depth, base
        |
        v  TopologyPrePass verify.txt topology.tsv [pack]     chain root + position per object,
        |                                                      optional base64 payload column
        v  PayloadVerifier / PackVerifier                      prove every object rebuilds, SHA-1 exact
        |
        v  TimestampAnnotator                                  + when each object entered history   (Stage 2)
        v  ChainOriginAnnotator                                + age of each chain's oldest object (Stage 4)
        |
        v  hdfs dfs -put  -->  hadoop jar DeltaChainResolver.jar ...
```

The columns grow stage by stage: 7 (topology), 8 (+ payload), 9 (+ object timestamp), 10 (+ chain origin). The mapper accepts all four.

### The five stages

| # | Stage | Key (sort order) | What it forced me to learn | Verified by |
|---|-------|------------------|----------------------------|-------------|
| 1 | Baseline secondary sort | root, position | Partitioner, sort comparator and grouping comparator are three separate classes with three separate jobs | Every chain delivered in position order; every object reconstructed and SHA-1 checked (`VERIFIED` 28,004, `FAILED` 0) |
| 2 | Multi-field composite sort | root, position, **timestamp** | What *groups* records (root) is different from what *orders* them (all fields) | Timestamps cross-checked against `git ls-tree`/`git cat-file`; 36 residual ties in 17 chains |
| 3 | Mixed-direction sort | root, position asc, **size desc**, timestamp | A hand-rolled `compareTo` that mixes directions; swap operands, never subtract | Reducer flags size going *up* within a position; in a local test, a deliberately flipped comparator was flagged in 297 chains; ties fell to 3 |
| 4 | Total order across reducers | **origin**, root, position, size desc, timestamp | `TotalOrderPartitioner` with a sampled boundary file; reducer 0 holds the oldest chains | Origins never decrease across `part-r-*`; 0 order violations; ranges disjoint |
| 5 | Bounded top-K per chain | (same as 4) | A heap that keeps the K worst-so-far instead of sorting everything | Every chain's top-5 recomputed independently by sort-then-truncate: all 6,472 match |

#### Stage 1 - baseline secondary sort
Composite key `(chainRootID, chainPosition)`. `ChainPartitioner` hashes the root alone, `ChainGroupingComparator` compares the root alone, and the key's own ordering sorts by the full key. Each `reduce()` call gets one chain, already in dependency order, with no in-memory sort. The reducer applies each delta to its already-rebuilt base and checks `sha1("<type> <size>\0" + content)`.

#### Stage 2 - multi-field composite sort
The key gains `objectTimestamp`: the committer time of the earliest commit that reaches the object (found by walking commits oldest-first and letting the first one claim each tree). Where chain position alone ties, the timestamp orders the records. Ties that remain (same position, same timestamp) are counted, not hidden.

#### Stage 3 - mixed-direction sort
Within a chain: position ascending (dependency order must go forward), then object size *descending* so the heaviest patch at each depth arrives first. Records at the same position never depend on each other, so this extra order is free. Descending is `Long.compare(other, this)`; `other - this` is the classic overflow bug.

#### Stage 4 - total order across reducers
Stages 1-3 only order records inside one reducer. Here the final output, read start to end across all reducers, is in chain-age order.
- A new leading key field, `chainOrigin` (age of the chain's oldest object), is identical on every record of a chain, so it orders chains without splitting any. A pre-pass computes it, since a mapper sees one line at a time.
- `ChainSampler` reservoir-samples the input and writes the partition-boundary file (`numReducers - 1` keys) that `TotalOrderPartitioner` reads.
- **Boundaries must be chain *starts*, not real records.** A boundary like `(origin, root, position 5)` would put positions 0-4 of a chain on one reducer and 5+ on the next. This was tested: with record-key boundaries, 3 chains were split and 343 objects failed verification.
- Hadoop's `InputSampler` was not usable because it samples the *input* format's keys (byte offsets), while the file must hold *map-output* keys.
- On the cluster with 2 reducers: reducer 0 holds chains whose oldest object dates from 1993-07-28 to 2002-08-16 (2,160 chains), reducer 1 from 2002-08-20 to 2026-10 (4,312 chains). In a local comparison run, hash partitioning gave every reducer the whole 1993..2026 span.

#### Stage 5 - bounded top-K per chain
`-Dchain.topk=K` swaps in `ChainTopKReducer`, which reports each chain's K most expensive deltas (largest patches) through `BoundedTopK`, a heap whose head is the worst item kept. Each delta is compared with only that head: add, replace, or discard. Nothing is sorted in full and then truncated.
- Memory per chain is O(K) instead of O(chain), and since patch size is already in the key, the mapper stops sending payloads. On the cluster the map output fell from 16.87 MB to 4.56 MB for the same 28,004 records.
- Trade-off: this reducer does not rebuild or SHA-check anything. Stages 1-4 proved the objects rebuild; Stage 5 is the cost report.
- The top 5 patches per chain carry 1,420,913 of the 3,918,045 patch bytes (36%). `BoundedTopKSelfCheck` compares the heap with sort-then-truncate on 1,920 randomized trials full of ties, and `TopKVerifier` recomputes the whole job's answer independently.

### Run it

Condensed; see [`DeltaChainResolver/README.md`](DeltaChainResolver/README.md) for the full steps and expected counters.

```sh
mvn -pl DeltaChainResolver -am clean package

# Stage 1-4: reconstruct + verify, total-order across reducers
hadoop jar DeltaChainResolver.jar /inputD /output_G 2            # 10-column input
hadoop jar DeltaChainResolver.jar /inputD /output_G 2 hash       # Stage 1-3 style hash partitioning

# Stage 5: K biggest patches per chain (the -D goes before the paths)
hadoop jar DeltaChainResolver.jar -Dchain.topk=5 /inputD /output_H 2
```

Numbers will differ slightly with a different repack: `git repack` with many threads is not deterministic (repacking the same repository gave 6,465 chains in one run and 6,472 in another), so always compare against the TSV you actually loaded.

### Things worth remembering
- Hadoop mutates the same key object while you iterate `values`; at `reduce()` entry the key already holds the group's first key.
- A counter that was never incremented is simply absent. "No `FAILED` line" means 0, but only trust it after checking the output is non-empty.
- `git verify-pack` reports a delta's *delta-stream* length as its size, not the rebuilt size.

### Possible future work
Other version-control stores use different delta formats (Mercurial revlogs, SVN deltas). The same chain-per-reducer idea applies once a pre-pass can emit `(root, position)` for them.

---

## A bit of Geospatial Information Systems and Supply Chain Investigation for Multimodal Shipment Tracking via Spatial Joins in MapReduce

**Status: planned, not started.**

GIS answers the question of where things are in a geographical world. One of the things that strikes me is how shipments globally work. Shipments aren't just flown via air cargo BUT through other means of transport such as naval (or shipments crossing the sea), transferring onto rail, finishing journey via truck, and so on.

How this might work:
1. AIS pings for ships
2. ADS-B for air freight
3. GPS traces for rail and road

All of those aren't easy to compound into a simple "join" in the way two tables work with some shared key or ID. A ship's position doesn't equal a port, it's *contained by* one, and a container's handoff between modes has to be inferred from proximity in space and time rather than matched on a key.

Since MapReduce is fundamentally about deriving structured pairings from a massive collection of raw data, here's what I was thinking.

"Real-world tracking data is really just a stream of (lat, lon, timestamp) points moving through space, and the reference infrastructure it needs to be joined against — ports, customs zones, airports, rail networks — is stored as polygons and line geometries, the actual building blocks of GIS. There's no shared key between a moving point and a stationary boundary, so the join has to be invented: bucket both the tracking pings and the reference polygons into a coarse spatial grid, and use the grid cell itself as the derived join key, turning an inherently keyless spatial relationship into something MapReduce can shuffle and group."

#### Ramp-up stages for Multimodal Shipment Tracking

**Stage 1 — Naive spatial join (grid-bucketed point-in-polygon)**
Mapper assigns every tracking ping and every reference polygon (ports, airports, customs zones) to a coarse spatial grid cell, replicating each polygon into every cell it intersects. Reducer performs the exact point-in-polygon test only against the candidate polygons sharing a ping's cell, establishing correctness before any optimization.

**Stage 2 — Three-way join across transport modes**
Extend the pipeline to chain a ship-at-port event (point-in-polygon) with a customs zone check (point-in-polygon against a second reference layer) and a cargo manifest record (exact-key join on container ID) in one pipeline — composing two different join mechanics with an exact-key join, either as one job with a multi-tagged mapper output or as chained jobs where one stage's output feeds the next.

**Stage 3 — Skew-resistant join**
A handful of megaports and hub airports (Shanghai, Singapore, LA/Long Beach) generate enormous ping volume relative to small regional ports and stations, overloading whichever reducer owns their grid cells. Detect this via a pre-pass job counting pings per grid cell, then salt the hottest cells into sub-keys distributed round-robin across multiple reducers, replicating the (small) polygon side across each salted partition so the join still resolves correctly.

**Stage 4 — Map-side join via DistributedCache**
Recognize that the reference layer — every port, airport, and rail station polygon — is small enough to hold entirely in memory relative to the enormous tracking-ping stream. Broadcast it via the DistributedCache, load it into a spatial lookup structure in the mapper's setup(), and resolve the point-in-polygon join in a single pass with no shuffle at all for this join.

**Stage 5 — Bloom filter pre-pass**
Before the full join runs, build a Bloom filter over "which grid cells actually contain a hub" from the reference layer. Distribute the filter and use it in the tracking-ping mapper to discard the vast majority of pings — the ones sitting mid-ocean or mid-flight, nowhere near any transfer point — before they ever reach the shuffle phase, cutting join volume down to only the pings that could plausibly matter.
