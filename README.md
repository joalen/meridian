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
