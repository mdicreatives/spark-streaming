package com.spark.streaming.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object SparkStreamingDirectory extends App{

  if (args.length < 5) {
    System.err.println("Usage: SparkStreamingDirectory <Input> <checkpoint_dir> <subtaskA_output> <subtaskB_output> <subtaskC_output>")
    System.exit(1)
  }
  val inputDirectory = args(0)
  val checkPointDir = args(1)
  val outputTaskA = args(2)
  val outputTaskB = args(3)
  val outputTaskC = args(4)


  // Spark configuration
  val conf = new SparkConf().setAppName("Spark HDFS Folder Streaming/Monitoring").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(10))
  // stateful transformation require check points
  ssc.checkpoint(checkPointDir)
  // Input file lines RDD
  val lines = ssc.textFileStream(inputDirectory)    /// LOCAL FILE SYSTEM
  //val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory) //HDFS
  lines.count().print()

  def updateFunction(newValues: Seq[Int], state: Option[Int]): Option[Int] =
     {
       val currentCount = newValues.sum
       val previousCount = state.getOrElse(0)
       Some(currentCount + previousCount)
     }


  // Task-a
  val wordCount = lines.flatMap( word => word.toString().split(" ").filter( word => !word.isEmpty))
  val outputTaskRDD = wordCount.filter(_.matches("\\w+"))
        .map(word => (word,1))
        .reduceByKey((total,inc) => total+inc )
  outputTaskRDD.count.print()
  outputTaskRDD.foreachRDD(rdd => {
        if (rdd.count() > 0){
          val processingTime = System.nanoTime()
          rdd.coalesce(1).saveAsTextFile(s"$outputTaskA/$processingTime")
        }
      }
      )

  val taskB= lines.filter(line => !line.toString().isEmpty ).map(line => line.toString().trim.split(" ")
    .filter(word =>  (word.length < 5))
    .filter(word => word.matches("\\w+"))
  ).flatMap(word => word.combinations(2))
    .map(el => ((el.head,el(1)),1))
    .reduceByKey((total,inc) => total+inc )

  val taskBp = taskB.map(el => el._1+"  "+el._2)
  taskBp.foreachRDD(rdd => {
    if (rdd.count() > 0){
      val processingTime = System.nanoTime()
      rdd.coalesce(1).saveAsTextFile(s"$outputTaskB/$processingTime")
    }
  }
  )

  val taskC = lines.filter(line => !line.toString().isEmpty ).map(line => line.toString().trim.split(" ")
    .filter(word =>  (word.length < 5))
    .filter(word => word.matches("\\w+"))
  ).flatMap(word => word.combinations(2))
    .map(el => ((el.head,el(1)),1))

  val taskCUpdate = taskC.updateStateByKey(updateFunction _)
  taskCUpdate.print()
  taskCUpdate.foreachRDD(rdd => {
    if (rdd.count() > 0){
      val processingTime = System.nanoTime()
      rdd.coalesce(1).saveAsTextFile(s"$outputTaskC/$processingTime")
    }
  }
  )

  ssc.start()
  ssc.awaitTermination()
}
