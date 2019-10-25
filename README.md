# Producing the Greatest Movie

CS 555 Term Project

Authors: Scott Axcell, Brandon Glidemaster, Daniel Kielman

## Build and Run

```
gradle assemble
./run.sh
```

## Hadoop

```
export HADOOP_CONF_DIR=<path-to-your-hadoop-config-dir>
./scripts/start-dfs.sh && ./scripts/start-yarn.sh
./scripts/stop-yarn.sh && ./scripts/stop-dfs.sh
```

## Spark

Setting up a spark cluster: https://www.cs.colostate.edu/~cs455/info-spaces.html#SSC

```
export SPARK_HOME=<path-to-your-spark-install>
$SPARK_HOME/sbin/start-all.sh
$SPARK_HOME/sbin/stop-all.sh
```
