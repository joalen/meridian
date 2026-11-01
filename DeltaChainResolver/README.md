# DeltaChainResolver

Parallel delta-chain resolution for git pack files, done with Hadoop MapReduce. Part of the Meridian repo; the root README has the cluster setup and the story behind the five stages.

Git stores many objects in a pack as a delta against a base object, which may itself be a delta, and so on. Rebuilding one object means walking its chain from the root outward, in order. A real pack has thousands of independent chains, so the chains can be resolved in parallel. This module does that with a secondary sort: the mapper tags each object with its chain root and position, a partitioner and grouping comparator send a whole chain to one reducer, and the shuffle sort hands it over in dependency order.

Test data: the Lua repository, repacked into one pack. 28,004 objects form 6,472 chains (21,532 deltas hanging off 6,472 full-content roots).

## Stages

| # | Stage | Sort key | Verified by |
|---|-------|----------|-------------|
| 1 | Baseline secondary sort | root, position | Every chain arrives in position order; every object rebuilds and passes SHA-1 |
| 2 | Multi-field composite sort | root, position, timestamp | Timestamps cross-checked against `git ls-tree` / `git cat-file` |
| 3 | Mixed-direction sort | root, position asc, size desc, timestamp | Reducer flags any size that goes up within a position |
| 4 | Total order across reducers | origin, root, position, size desc, timestamp | Origins never decrease across `part-r-*`; ranges disjoint |
| 5 | Bounded top-K per chain | same as 4 | Every chain's top-K matches sort-then-truncate |

Three classes do three separate jobs and are registered separately in the driver: `ChainPartitioner` (which reducer), `ChainSortComparator` (record order), `ChainGroupingComparator` (which records share one `reduce()` call).

## Prerequisites

- Hadoop cluster from the root README (namenode, datanode, resourcemanager, nodemanager)
- JDK 8, Maven, Git
- `DeltaChainResolver` listed in the root `pom.xml` modules

## Build

From the repository root:

```sh
mvn -pl DeltaChainResolver -am clean package
```

Produces `DeltaChainResolver/target/DeltaChainResolver.jar`. The pre-pass tools below are in the same jar and run with plain `java -cp`, no cluster needed.

## Prepare the input

Hadoop cannot read a `.pack` directly, so a few local tools produce a TSV first.

```sh
# 1. In a clone of the repo you want to analyze: one big pack with long chains
git clone <repository>.git && cd <repository>
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

Columns grow by stage: 7 (topology), 8 (+ payload), 9 (+ object timestamp), 10 (+ chain origin). The mapper accepts all four.

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

## View / export results

```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat '/output_G/part-r-*' > output_G.txt
```

## Verify

Expected on the Lua test data (your numbers may vary slightly with a different repack):

- Stages 1-4: `VERIFIED` 28,004, no `FAILED` counter, `GROUP_ORDER_VIOLATIONS` 0, `OUT_OF_ORDER_GROUPS` 0
- Stage 4 with 2 reducers: reducer 0 holds the oldest chains (1993-07-28 to 2002-08-16, 2,160 chains), reducer 1 the rest (2002-08-20 onward, 4,312 chains)
- Stage 5: map output drops from 16.87 MB to 4.56 MB for the same 28,004 records

Independent checks that need no cluster:

```sh
java -cp DeltaChainResolver.jar com.joalen.chain.BoundedTopKSelfCheck
cat output_H.txt > merged.txt   # all part-r-* files
java -cp DeltaChainResolver.jar com.joalen.pack.TopKVerifier topology_origin.tsv merged.txt 5
```

`TopKVerifier` should report all 6,472 chains match. `BoundedTopKSelfCheck` compares the heap with sort-then-truncate on randomized trials full of ties.

## Notes

- The output directory (and `<out>_partitions`) must not already exist. Clear with `hdfs dfs -rm -r <dir>`.
- A counter that was never incremented is absent, so "no `FAILED` line" means 0. Check the output is non-empty before trusting that.
- Partition boundaries are chain starts, never real records. A boundary inside a chain would split it across reducers and break reconstruction.
- `git repack` with many threads is not deterministic: the same repo gave 6,465 chains in one run and 6,472 in another. Compare against the TSV you actually loaded.
- Stage 5 does not rebuild or SHA-check anything; Stages 1-4 already proved the objects rebuild.
- Hadoop mutates the same key object while you iterate `values`, so read `key.getChainPosition()` etc. inside the loop.
- `git verify-pack` reports a delta's delta-stream length as its size, not the rebuilt size.