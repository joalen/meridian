# ReduceSideJoin

Joins `dataset.csv` with `dataset2.csv` **in the reducer**, producing the average temperature of each state's capital city. It is the reduce-side counterpart to `MapSideJoin`.

## How it works
1. `MultipleInputs` assigns each file its own mapper:
   - `TemperatureMapper` emits `(state, "T<TAB>city<TAB>temp")`
   - `CapitalMapper` emits `(state, "C<TAB>capital")`
2. The shuffle brings all records for one state to the same reducer.
3. `JoinReducer` takes the `C` record as the capital and sums temperatures per city. It emits the average **only for the capital city**. States with no capital record, or no readings for their capital, produce no output.

Rows with an unparsable temperature, `-99`, or an empty state are skipped, and header rows are ignored.

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
mvn -pl ReduceSideJoin -am clean package
```
Produces `ReduceSideJoin/target/ReduceSideJoin.jar`.

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
docker cp ReduceSideJoin/target/ReduceSideJoin.jar resourcemanager:/tmp/ReduceSideJoin.jar

# Usage: hadoop jar ReduceSideJoin.jar <in> <out>
docker exec -it resourcemanager hadoop jar /tmp/ReduceSideJoin.jar /inputC /reduce_join_output
```

## View / export results
```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat /reduce_join_output/part-r-00000 > reduce_join_output.txt
```

## Notes
- The output directory must not already exist.
- Unlike `MapSideJoin`, no cache file is needed. The trade-off is that the join data goes through the shuffle, and each state's temperature records are grouped in reducer memory.
- Compare against `MapSideJoin` with `diff map_join_output.txt reduce_join_output.txt`. They should match.