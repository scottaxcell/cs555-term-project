SPARK_MASTER=frankfort:31312

gradle assemble

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/BudgetAnalysis
$SPARK_HOME/bin/spark-submit \
--master spark://${SPARK_MASTER} \
--deploy-mode cluster \
--class cs555.project.drivers.BudgetDriver \
--supervise build/libs/cs555-term-project.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/CastAnalysis
$SPARK_HOME/bin/spark-submit \
--master spark://${SPARK_MASTER} \
--deploy-mode cluster \
--class cs555.project.drivers.CastDriver \
--supervise build/libs/cs555-term-project.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/OverviewAnalysis
$SPARK_HOME/bin/spark-submit \
--master spark://${SPARK_MASTER} \
--deploy-mode cluster \
--class cs555.project.drivers.OverviewDriver \
--supervise build/libs/cs555-term-project.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/TaglineAnalysis
$SPARK_HOME/bin/spark-submit \
--master spark://${SPARK_MASTER} \
--deploy-mode cluster \
--class cs555.project.drivers.TaglineDriver \
--supervise build/libs/cs555-term-project.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/TitleAnalysis
$SPARK_HOME/bin/spark-submit \
--master spark://${SPARK_MASTER} \
--deploy-mode cluster \
--class cs555.project.drivers.TitleDriver \
--supervise build/libs/cs555-term-project.jar