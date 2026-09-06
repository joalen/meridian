# MapReduce Setup Instructions

Here we have a two Apache Hadoop MapReduce jobs, Q1Analysis and Q2Analysisrespectively, that run against the official [Apache Hadoop](https://github.com/apache/hadoop) docker compose file and spawns its nodes: NameNode, DataNode, ResourceManager, NodeManager. 

## Pre-Requisites
- Docker + Docker Compose
- JDK 8
- Git 
- Hadoop cluster names namenode, datanode, resourcemanager, nodemanager

## General Steps 
1. `git clone -b "docker-hadoop-3" https://github.com/apache/hadoop hadoop`
2. `cd hadoop` 
3. Edit the docker-compose.yml file to add `containername: namenode` to namenode service and `containername: resourcemanager` to resourcemanager service
4. Edit the config file to point to /opt/hadoop 
5. Startup Hadoop: `docker compose up -d`
6. Confirm containers: `docker ps`

## Part #1 Steps 
1. Retrieve q1_dataset.txt and place it in current working directory OR run `curl -o q1_dataset.txt -L https://www.gutenberg.org/cache/epub/2701/pg2701.txt`
2. `docker cp q1_dataset.txt namenode:/tmp/q1_dataset.txt`
3. Retrieve the location of JAVA_HOME and set accordingly like so. Example from mine on a vscode devcontainer for Java:
```sh
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputA && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/q1_dataset.txt /inputA/'
```
4. Retrieve Q1Analysis.jar and place it in current working directory. 
5. `docker cp Q1Analysis.jar resourcemanager:/tmp/Q1Analysis.jar`
6. `docker exec -it resourcemanager hadoop jar /tmp/Q1Analysis.jar /inputA /q1_output_A A`
7. `docker exec resourcemanager sh -c   '/opt/hadoop/bin/hdfs dfs -cat /q1_output_A/part-r-00000 > /tmp/q1_output_A.txt 2>&1'`
8. `docker cp resourcemanager:/tmp/q1_output_A.txt .`
9. Repeat steps 7 for part B (q1_output_B B) and C (q1_output_C C) and 8 to where you change the name to match either q1_output_B or q1_output_C respectively for the directory and filename

## Part #2 Steps 
1. Retrieve q2_dataset.txt and place it in current working directory. 
2. `docker cp q2_dataset.txt namenode:/tmp/q2_dataset.txt`
3. Retrieve the location of JAVA_HOME and set accordingly like so. Example from mine on a vscode devcontainer for Java:
```sh
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputB && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/q2_dataset.txt /inputB/'
```
4. Retrieve Q2Analysis.jar and place it in current working directory.
5. `docker cp Q2Analysis.jar resourcemanager:/tmp/Q2Analysis.jar`
6. `docker exec -it resourcemanager hadoop jar /tmp/Q2Analysis.jar /inputB /q2_output_A A`
7. `docker exec resourcemanager sh -c   '/opt/hadoop/bin/hdfs dfs -cat /q2_output_A/part-r-00000 > /tmp/q2_output_A.txt 2>&1'`
8. `docker cp resourcemanager:/tmp/q2_output_A.txt .`
9. `docker exec -it resourcemanager hadoop jar /tmp/Q2Analysis.jar /q2_output_A /q2_output_B B`
10. `docker cp resourcemanager:/tmp/q2_output_B.txt .`


### A bit of Geospatial Information Systems and Supply Chain Investigation for Multimodal Shipment Tracking via Spatial Joins in MapReduce
GIS answers the question of where things are in a geographical world. One of the things that strikes me is how shipments globally work. Shipments aren't just flown via air cargo BUT through other means of transport such as naval (or shipments crossing the sea), transferring onto rail, finishing journey via truck, and so on. 

How this might work: 
1. AIS pings for ships 
2. ADS-B for air frieght
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

### Parallel Delta Chain Resolution via MapReduce (Git pack files)
One of the interesting things about git is how it is able to send objects for when you do repository push/pull/rebase/etc. How it sends all of these items over the wire is through pack files. 

Since MapReduce is the idea of taking a giant collection of data and rendering back pairings from data provided some end goal, here's what I was thinking. 

"Within git pack files are delta chains: each object may be stored not in full, but as a diff against a "base" object, which may itself be a diff against another base, and so on. Reconstructing any single object means walking its chain from the root outward, strictly in order, posing an arduous challenge into sequentially constructing this. However, real pack files usually contain thousands of independent chains, all resolvable in parallel.

Therefore, to tap into secondary sort where I can control sort ordering within a reduce group, I'll be doing the following stages of the mapper parses a pack file's index and emits each delta tagged with (chainRootID, chainPosition); a custom partitioner and grouping comparator route every delta in a chain to the same reducer while preserving dependency order via Hadoop's shuffle sort. The reducer then streams through each chain, applying deltas in sequence to reconstruct the final object.

#### Ramp-up stages for Parallel Delta Chain Resolution
**Stage 1 — Baseline secondary sort**
Composite key (chainRootID, chainPosition); custom partitioner groups by chainRootID alone, custom comparator sorts by the full key. Each reducer receives one chain's deltas pre-ordered by dependency position, with no in-memory sort. This establishes the core secondary-sort machinery — partitioner, grouping comparator, sort comparator — as three genuinely distinct classes doing three distinct jobs.

**Stage 2 — Multi-field composite sort**
Extend the key to (chainRootID, chainPosition, objectTimestamp), so that where a pack file's delta ordering ties or is ambiguous (e.g., repacked history where original commit order isn't preserved in chain position alone), a second sort field breaks ties deterministically. This forces the comparator to handle a genuine multi-field ordering instead of a single tiebreaker, and separates "what groups records together" from "what fully orders them" more sharply than Stage 1 does.

**Stage 3 — Mixed-direction sort**
Within a chain, sort chainPosition ascending (dependency order must go forward) but break ties by object size *descending* — e.g., when investigating which deltas in a chain are the heaviest to resolve. This requires a custom WritableComparable whose compareTo mixes ascending and descending logic across its fields, rather than relying on Hadoop's default natural ordering, and is a good forcing function for writing a correct, hand-rolled comparator instead of composing built-in ones.

**Stage 4 — Total order across reducers, not just within them**
Stages 1–3 only guarantee order *inside* a single reducer's chain. Extend the pipeline so that the final output file, read start to end across all reducers, is in a single globally meaningful order — e.g., every chain's resolved objects emitted in overall commit-history order, not grouped arbitrarily by reducer. This means moving from a hash partitioner to a TotalOrderPartitioner with a sampled partition-boundary file, so reducer 0 holds the globally-earliest range of keys, reducer 1 the next range, and so on — a materially different partitioning strategy than anything in Stages 1–3.

**Stage 5 — Bounded top-K sort per group**
Rather than materializing every delta in a chain, keep only the K most expensive-to-resolve deltas per chain (e.g., the largest patches, to flag which objects are costing the most reconstruction work). Implement this as a bounded in-reducer priority queue that maintains sort order incrementally rather than sorting the full chain and truncating — the sort and the truncation happen together, which is a different problem than sorting everything and throwing away the tail.