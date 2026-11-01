# TextMetrics

Three Hadoop MapReduce jobs over a plain-text corpus (developed against *Moby Dick*, Project Gutenberg #2701). Text is lowercased and split on any non-letter character.

| Part | What it computes | Output line format |
|------|------------------|--------------------|
| `A` | Frequency of every word | `word<TAB>count` |
| `B` | Frequency of only `ahab`, `captain`, `harpoon` | `word<TAB>count` |
| `C` | For each (word length, last letter), the number of **distinct** words | `length,lastChar<TAB>distinctCount` |

## Prerequisites
- Hadoop cluster running (see the root README)
- JDK 8 and Maven (only needed to build)

## Build
From the repository root:
```sh
mvn -pl TextMetrics -am clean package
```
Produces `TextMetrics/target/TextMetrics.jar`.

## Load the data
```sh
curl -o dataset.txt -L https://www.gutenberg.org/cache/epub/2701/pg2701.txt
docker cp dataset.txt namenode:/tmp/dataset.txt
docker exec -e JAVA_HOME=/usr/lib/jvm/jre/ namenode sh -c \
  '/opt/hadoop/bin/hdfs dfs -mkdir -p /inputA && \
   /opt/hadoop/bin/hdfs dfs -put -f /tmp/dataset.txt /inputA/'
```
Adjust `JAVA_HOME` to match your container.

## Run
```sh
docker cp TextMetrics/target/TextMetrics.jar resourcemanager:/tmp/TextMetrics.jar

# Usage: hadoop jar TextMetrics.jar <in> <out> <part>
docker exec -it resourcemanager hadoop jar /tmp/TextMetrics.jar /inputA /output_A A
docker exec -it resourcemanager hadoop jar /tmp/TextMetrics.jar /inputA /output_B B
docker exec -it resourcemanager hadoop jar /tmp/TextMetrics.jar /inputA /output_C C
```

## View / export results
```sh
docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -cat /output_A/part-r-00000 > output_A.txt
```
Repeat with `output_B` and `output_C`.

## Notes
- The output directory must **not** already exist. To rerun:
  `docker exec resourcemanager /opt/hadoop/bin/hdfs dfs -rm -r /output_A`
- An invalid `<part>` exits with code 2.