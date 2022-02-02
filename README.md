SPARK STREAMING EXAMPLE DStream 
[Using localfiles as input]

1. Create Input HDFS directory using command below or using Namenode UI
hdfs dfs -mkdir /spark 
hdfs dfs -mkdir /spark/input

2. DStream library picks files that are modified after the stream session has started. spark-submit command mentioned below needs to be updated with appropriate enviornment configurations(For reference please see example command used to run the job in local enviornment)

spark-submit \
--master yarn \
--class com.spark.streaming.scala.SparkStreamingDirectory \
--deploy-mode client
s3://<path-to-jar>/spark-streaming-assignment.jar \
<hdfs-input-path> <hdfs-output-taskA-path> <hdfs-output-taskB-path>

Example:

spark-submit \
--master local[2] \
--class com.spark.streaming.scala.SparkStreamingDirectory \
/home/user/spark-streaming-assignment/out/artifacts/spark_streaming_assignment_jar/spark-streaming-assignment.jar \
hdfs://127.0.0.1:9000/spark/input hdfs://127.0.0.1:9000/spark/output/taskA hdfs://127.0.0.1:9000/spark/output/taskB


Wait for few seconds till the streaming session starts and place the file in input HDFS directory.

hdfs dfs -put 3littlepigs2 /spark/input/


