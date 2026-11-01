# DeltaChainResolver (an exploration into parallel delta chain resolutions)

**Status: all five stages implemented and verified on the cluster.**

Parallel delta-chain resolution for git pack files, done with Hadoop MapReduce. Part of the Meridian repo; the root README has the cluster setup.

One of the interesting things about git is how it sends objects for push, pull, rebase and so on: through pack files. Within a pack file are delta chains. Each object may be stored not in full, but as a diff against a "base" object, which may itself be a diff against another base, and so on. Reconstructing any single object means walking its chain from the root outward, strictly in order. However, real pack files usually contain thousands of independent chains, all resolvable in parallel.

That is a natural fit for MapReduce's secondary sort: the mapper tags each object with `(chainRootID, chainPosition)`, a custom partitioner and grouping comparator route a whole chain to one reducer, and Hadoop's shuffle sort delivers it in dependency order.

## The data

The test repository is Lua: `28,004` objects in one repacked pack, `5,938` commits. On the cluster those objects form `6,472` chains: `21,532` deltas hanging off `6,472` full-content roots. The longest chain history spans 29 years.

## The five stages

| # | Stage | Key (sort order) | What it forced me to learn | Verified by |
|---|-------|------------------|----------------------------|-------------|
| 1 | Baseline secondary sort | root, position | Partitioner, sort comparator and grouping comparator are three separate classes with three separate jobs | Every chain delivered in position order; every object reconstructed and SHA-1 checked (`VERIFIED` 28,004, `FAILED` 0) |
| 2 | Multi-field composite sort | root, position, **timestamp** | What *groups* records (root) is different from what *orders* them (all fields) | Timestamps cross-checked against `git ls-tree`/`git cat-file`; 36 residual ties in 17 chains |
| 3 | Mixed-direction sort | root, position asc, **size desc**, timestamp | A hand-rolled `compareTo` that mixes directions; swap operands, never subtract | Reducer flags size going *up* within a position; in a local test, a deliberately flipped comparator was flagged in 297 chains; ties fell to 3 |
| 4 | Total order across reducers | **origin**, root, position, size desc, timestamp | `TotalOrderPartitioner` with a sampled boundary file; reducer 0 holds the oldest chains | Origins never decrease across `part-r-*`; 0 order violations; ranges disjoint |
| 5 | Bounded top-K per chain | (same as 4) | A heap that keeps the K worst-so-far instead of sorting everything | Every chain's top-5 recomputed independently by sort-then-truncate: all 6,472 match |

The three shuffle classes are registered separately in the driver: `ChainPartitioner` (which reducer), `ChainSortComparator` (record order), `ChainGroupingComparator` (which records share one `reduce()` call).

### Stage 1 - baseline secondary sort
Composite key `(chainRootID, chainPosition)`. `ChainPartitioner` hashes the root alone, `ChainGroupingComparator` compares the root alone, and the key's own ordering sorts by the full key. Each `reduce()` call gets one chain, already in dependency order, with no in-memory sort. The reducer applies each delta to its already-rebuilt base and checks `sha1("<type> <size>\0" + content)`.

### Stage 2 - multi-field composite sort
The key gains `objectTimestamp`: the committer time of the earliest commit that reaches the object (found by walking commits oldest-first and letting the first one claim each tree). Where chain position alone ties, the timestamp orders the records. Ties that remain (same position, same timestamp) are counted, not hidden.

### Stage 3 - mixed-direction sort
Within a chain: position ascending (dependency order must go forward), then object size *descending* so the heaviest patch at each depth arrives first. Records at the same position never depend on each other, so this extra order is free. Descending is `Long.compare(other, this)`; `other - this` is the classic overflow bug.

### Stage 4 - total order across reducers
Stages 1-3 only order records inside one reducer. Here the final output, read start to end across all reducers, is in chain-age order.
- A new leading key field, `chainOrigin` (age of the chain's oldest object), is identical on every record of a chain, so it orders chains without splitting any. A pre-pass computes it, since a mapper sees one line at a time.
- `ChainSampler` reservoir-samples the input and writes the partition-boundary file (`numReducers - 1` keys) that `TotalOrderPartitioner` reads.
- **Boundaries must be chain *starts*, not real records.** A boundary like `(origin, root, position 5)` would put positions 0-4 of a chain on one reducer and 5+ on the next. This was tested: with record-key boundaries, 3 chains were split and 343 objects failed verification.
- Hadoop's `InputSampler` was not usable because it samples the *input* format's keys (byte offsets), while the file must hold *map-output* keys.
- On the cluster with 2 reducers: reducer 0 holds chains whose oldest object dates from 1993-07-28 to 2002-08-16 (2,160 chains), reducer 1 from 2002-08-20 to 2026-10 (4,312 chains). In a local comparison run, hash partitioning gave every reducer the whole 1993..2026 span.

### Stage 5 - bounded top-K per chain
`-Dchain.topk=K` swaps in `ChainTopKReducer`, which reports each chain's K most expensive deltas (largest patches) through `BoundedTopK`, a heap whose head is the worst item kept. Each delta is compared with only that head: add, replace, or discard. Nothing is sorted in full and then truncated.
- Memory per chain is O(K) instead of O(chain), and since patch size is already in the key, the mapper stops sending payloads. On the cluster the map output fell from 16.87 MB to 4.56 MB for the same 28,004 records.
- Trade-off: this reducer does not rebuild or SHA-check anything. Stages 1-4 proved the objects rebuild; Stage 5 is the cost report.
- The top 5 patches per chain carry 1,420,913 of the 3,918,045 patch bytes (36%). `BoundedTopKSelfCheck` compares the heap with sort-then-truncate on 1,920 randomized trials full of ties, and `TopKVerifier` recomputes the whole job's answer independently.

## How the data gets to the cluster

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

## Prerequisites

- Hadoop cluster from the root README (namenode, datanode, resourcemanager, nodemanager)
- JDK 8, Maven, Git
- `DeltaChainResolver` listed in the root `pom.xml` modules

## Build

From the repository root:

```sh
mvn -pl DeltaChainResolver -am clean package
```

Produces `DeltaChainResolver/target/DeltaChainResolver.jar`. The pre-pass tools are in the same jar and run with plain `java -cp`, no cluster needed.

## Prepare the input

```sh
# 1. In a clone of the repo you want to analyze: one big pack with long chains
git repack -a -d -f --depth=250 --window=250
git verify-pack -v .git/objects/pack/pack-*.idx > verify.txt

# 2. Topology + payload (7 columns, or 8 with the pack argument)
java -cp DeltaChainResolver.jar com.joalen.prepass.TopologyPrePass \
  verify.txt topology.tsv .git/objects/pack/pack-XXXX.pack

# 3. Prove every object rebuilds, SHA-1 exact (both run without a cluster)
java -cp DeltaChainResolver.jar com.joalen.pack.PackVerifier  pack-XXXX.pack topology7.tsv   # 7-col input
java -cp DeltaChainResolver.jar com.joalen.pack.PayloadVerifier topology.tsv                  # 8-col input

# 4. Stage 2: add object timestamps (9 columns)
java -cp DeltaChainResolver.jar com.joalen.prepass.TimestampAnnotator topology.tsv topology_ts.tsv

# 5. Stage 4: add chain origin (10 columns)
java -cp DeltaChainResolver.jar com.joalen.prepass.ChainOriginAnnotator topology_ts.tsv topology_origin.tsv
```

## Load the data

```sh
docker cp topology_origin.tsv namenode:/tmp/topology_origin.tsv
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputD && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/topology_origin.tsv /inputD/'
```

## Run

```sh
docker cp DeltaChainResolver/target/DeltaChainResolver.jar resourcemanager:/tmp/DeltaChainResolver.jar

# Usage: hadoop jar DeltaChainResolver.jar [-Dchain.topk=K] <in> <out> [numReducers=4] [total|hash]

# Stages 1-4: reconstruct + SHA-1 verify, total order across reducers (needs 10-column input)
docker exec -it resourcemanager hadoop jar /tmp/DeltaChainResolver.jar /inputD /output_G 2

# Stages 1-3 style: hash partitioning (works with 7-10 column input)
docker exec -it resourcemanager hadoop jar /tmp/DeltaChainResolver.jar /inputD /output_G 2 hash

# Stage 5: report the K biggest patches per chain (-D goes before the paths)
docker exec -it resourcemanager hadoop jar /tmp/DeltaChainResolver.jar -Dchain.topk=5 /inputD /output_H 2
```

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `numReducers` | 4 | More than one on purpose: with a single reducer a broken partitioner is invisible |
| `total` / `hash` | `total` | `total` uses `TotalOrderPartitioner` with a sampled boundary file written to `<out>_partitions`; `hash` hashes the chain root |
| `-Dchain.topk=K` | 0 (off) | Swap in `ChainTopKReducer`; the mapper also stops sending payloads |
| `-Dchain.sample.size` | 2000 | Reservoir sample size for the boundary file |
| `-Dchain.sample.seed` | 42 | Sampling seed |

## Output

One line per chain: `rootSha<TAB>diagnostics`.

Stages 1-4 line: `n= maxPos= ordered= verified= failed= positions= origin= patchBytes= biggestPatch=` (plus `oldest= newest= ties=` for timestamped input, and `firstError=` on failure).

Stage 5 line: `n= deltas= k= kept= patchBytes= topBytes= top=<size>@<pos>:<sha7>,... origin=`

In total-order mode, read `part-r-00000`, `part-r-00001`, ... in order and the chains are in ascending origin (age) order.

The driver also prints this project's counters on stdout as `COUNTER <name> <value>`, plus `MAP_OUTPUT_BYTES` and `MAP_OUTPUT_RECORDS`.

To export results:

```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat '/output_G/part-r-*' > output_G.txt
```

## Verify

Expected on the Lua test data (numbers will differ slightly with a different repack):

- Stages 1-4: `VERIFIED` 28,004, no `FAILED` counter, `GROUP_ORDER_VIOLATIONS` 0, `OUT_OF_ORDER_GROUPS` 0
- Stage 4 with 2 reducers: reducer 0 holds the oldest chains (2,160), reducer 1 the rest (4,312)
- Stage 5: map output drops from 16.87 MB to 4.56 MB for the same 28,004 records

Independent checks that need no cluster:

```sh
java -cp DeltaChainResolver.jar com.joalen.chain.BoundedTopKSelfCheck
cat output_H.txt > merged.txt   # all part-r-* files
java -cp DeltaChainResolver.jar com.joalen.pack.TopKVerifier topology_origin.tsv merged.txt 5
```

`TopKVerifier` should report all 6,472 chains match.

## Things worth remembering

- The output directory (and `<out>_partitions`) must not already exist. Clear with `hdfs dfs -rm -r <dir>`.
- Hadoop mutates the same key object while you iterate `values`; at `reduce()` entry the key already holds the group's first key.
- A counter that was never incremented is simply absent. "No `FAILED` line" means 0, but only trust it after checking the output is non-empty.
- `git verify-pack` reports a delta's *delta-stream* length as its size, not the rebuilt size.
- `git repack` with many threads is not deterministic (repacking the same repository gave 6,465 chains in one run and 6,472 in another), so always compare against the TSV you actually loaded.

## Possible future work

Other version-control stores use different delta formats (Mercurial revlogs, SVN deltas). The same chain-per-reducer idea applies once a pre-pass can emit `(root, position)` for them.