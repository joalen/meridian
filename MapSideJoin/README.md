# MapSideJoin

Joins `dataset.csv` with `dataset2.csv` **in the mapper** using Hadoop's distributed cache. The result is the average temperature of each state's capital city.

## How it works
1. The driver registers `dataset2.csv` as a cache file (`#dataset2.csv` creates a local symlink with that name).
2. In `setup()`, each mapper loads it into an in-memory `HashMap<state, capital>`. It auto-detects `state` / `capital` header columns, and otherwise assumes column 0 = state and column 1 = capital.
3. In `map()`, a temperature row is emitted only if its city equals the capital of its state. Rows with an unparsable temperature or `-99` are skipped.
4. A combiner and a single reducer compute the average.

No shuffle is needed for the join itself, which works because the capitals table is small enough to fit in memory.

## Input
`<in>` is an HDFS **directory** containing both:
- `dataset.csv`
- `dataset2.csv`

## Output
```
<state><TAB><capital city><TAB><average, 2 decimals>
```

## Build
From the repository root:
```sh
mvn -pl MapSideJoin -am clean package
```
Produces `MapSideJoin/target/MapSideJoin.jar`.

## Load the data
```sh
docker cp dataset.csv namenode:/tmp/dataset.csv
docker cp dataset2.csv   namenode:/tmp/dataset2.csv
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputC && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/dataset.csv /tmp/dataset2.csv /inputC/'
```

## Run
```sh
docker cp MapSideJoin/target/MapSideJoin.jar resourcemanager:/tmp/MapSideJoin.jar

# Usage: hadoop jar MapSideJoin.jar <in> <out>
docker exec -it resourcemanager hadoop jar /tmp/MapSideJoin.jar /inputC /map_join_output
```

## View / export results
```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat /map_join_output/part-r-00000 > map_join_output.txt
```

## Notes
- The job fails in `setup()` with `dataset2.csv was not added to cache` if the file is missing from `<in>`.
- The output directory must not already exist.
- This module and `ReduceSideJoin` compute the same result by different strategies, so their outputs can be compared with `diff`.