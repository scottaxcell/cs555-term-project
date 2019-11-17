rm -f results/*/part-00000

$HADOOP_HOME/bin/hdfs dfs -get /user/${USER}/BudgetAnalysis/part-00000 results/BudgetAnalysis
$HADOOP_HOME/bin/hdfs dfs -get /user/${USER}/CastAnalysis/part-00000 results/CastAnalysis
$HADOOP_HOME/bin/hdfs dfs -get /user/${USER}/OverviewAnalysis/part-00000 results/OverviewAnalysis
$HADOOP_HOME/bin/hdfs dfs -get /user/${USER}/TaglineAnalysis/part-00000 results/TaglineAnalysis
$HADOOP_HOME/bin/hdfs dfs -get /user/${USER}/TitleAnalysis/part-00000 results/TitleAnalysis