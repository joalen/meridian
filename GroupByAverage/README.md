# GroupByAverage

A MapReduce group-by-and-average over `dataset.csv`: the mean daily temperature for each **(state, year)** in the **West** region.

## Input
`<in>` is an HDFS **directory** that must contain `dataset.csv`. Expected columns (0-indexed):

| Index | Field |
|-------|-------|
| 0 | Region |
| 2 | State |
| 6 | Year |
| 7 | AvgTemperature |

Rows are dropped if the region is not exactly `West`, the state or year is empty, the temperature is unparsable, or the temperature is `-99` (the dataset's missing-value sentinel). Header rows are ignored.

## Output
```
<state><TAB><year><TAB><average, 2 decimals>
```
A combiner pre-aggregates `(sum,count)` pairs so the average is computed correctly across mappers. One reducer is used, so the output is a single `part-r-00000`.

## Build
From the repository root:
```sh
mvn -pl GroupByAverage -am clean package
```
Produces `GroupByAverage/target/GroupByAverage.jar`.

## Load the data
```sh
docker cp dataset.csv namenode:/tmp/dataset.csv
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputC && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/dataset.csv /inputC/'
```

## Run
```sh
docker cp GroupByAverage/target/GroupByAverage.jar resourcemanager:/tmp/GroupByAverage.jar

# Usage: hadoop jar GroupByAverage.jar <in> <out>
docker exec -it resourcemanager hadoop jar /tmp/GroupByAverage.jar /inputC /group_output
```

## View / export results
```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat /group_output/part-r-00000 > group_output.txt
```

## Notes
- The output directory must not already exist (`hdfs dfs -rm -r /group_output` to rerun).