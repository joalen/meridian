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
