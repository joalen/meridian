# FieldIndexer

Two chained MapReduce jobs over a comma-separated dataset: build an inverted index, then find the "long" word that appears on the most lines.

| Part | Input | What it computes | Output line format |
|------|-------|------------------|--------------------|
| `A` | The raw dataset | Inverted index: each word maps to the sorted, de-duplicated line numbers where it appears | `word<TAB>n1, n2, n3, ...` |
| `B` | **Output directory of Part A** | Among words with 12+ letters, the one with the most line numbers | `word<TAB>count` |

## Input expectations (Part A)
- Column 0 is a numeric line ID. Rows with a non-numeric ID (e.g. headers) or fewer than 10 fields are skipped.
- Columns **1, 2, 4, 5** are tokenized (lowercased, split on non-letters). Empty fields are ignored.

## Build
From the repository root:
```sh
mvn -pl FieldIndexer -am clean package
```
Produces `FieldIndexer/target/FieldIndexer.jar`.

## Load the data
```sh
docker cp dataset.txt namenode:/tmp/dataset.txt
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputB && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/dataset.txt /inputB/'
```

## Run
```sh
docker cp FieldIndexer/target/FieldIndexer.jar resourcemanager:/tmp/FieldIndexer.jar

# Part A: build the inverted index
docker exec -it resourcemanager hadoop jar /tmp/FieldIndexer.jar /inputB /output_A A

# Part B: reads Part A's output directory
docker exec -it resourcemanager hadoop jar /tmp/FieldIndexer.jar /output_A /output_B B
```
Part B must run after Part A has finished with the same steps respectively.

## View / export results
```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat /output_A/part-r-00000 > output_A.txt
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat /output_B/part-r-00000 > output_B.txt
```

## Notes
- Output directories must not already exist (`hdfs dfs -rm -r <dir>` to clear).
- Part B uses a single constant key so every candidate reaches one reducer. A combiner keeps only the best candidate per mapper, so shuffle volume stays tiny.
- If two words tie for the highest count, the first one encountered wins.