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