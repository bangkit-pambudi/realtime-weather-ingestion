# Realtime weather kafka ingestion

Requirement : Python 3.7, Pyspark 2.X, and Ubuntu 20.04

## 1. Install Kafka

``` bash
wget https://archive.apache.org/dist/kafka/2.1.1/kafka_2.12-2.1.1.tgz
tar -xvf kafka_2.12-2.1.1.tgz
mv kafka_2.12-2.1.1.tgz kafka
sudo apt install openjdk-8-jdk -y
pip3 install kafka-python 
```

## 2. Install Cassandra
``` bash
wget https://dlcdn.apache.org/cassandra/4.0.7/apache-cassandra-4.0.7-bin.tar.gz
tar -xvf apache-cassandra-4.0-7-bin.tar.gz
mv apache-cassandra-4.0-7-bin cassandra
nano .bashrc
export CASSANDRA_HOME=/home/[USER]/cassandra
export PATH=$PATH:$CASSANDRA_HOME/bin
source .bashrc
cassandra
cqlsh
```

## 3. Install Pyspark
``` bash
tar -xvf Downloads/spark-2.4.3-bin-hadoop2.7.tgz
mv spark-2.4.3-bin-hadoop2.7.tgz spark
sudo apt install scala -y
scala -version
pip3 install pyspark=2.4.6
nano .bashrc
export PATH=$PATH:/home/<USER>/spark/bin
export PYSPARK_PYTHON=python3
source .bashrc
cd spark
bin/spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.2
cd <REPO_PATH>/ConsumerSpark/
wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.6/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar
```

## 4. How to Use
``` bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic weather
python3 Producer/main.py
cd <REPO_PATH>/ConsumerSpark/
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 transformer.py
```
