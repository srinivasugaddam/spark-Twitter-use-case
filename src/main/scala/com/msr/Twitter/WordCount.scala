package com.msr.Twitter

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args:Array[String]){
   
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
  val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
  val words = lines.flatMap(_.split(" "))

  // Count each word in each batch
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()

  ssc.start()          
  ssc.awaitTermination()
     
  }
   
}

/*
 * how to run
 * To run this on your local machine, you need to first run a Netcat server
 * nc -lk 9999
 * Then run the below spark submit command
 * spark-submit --class com.msr.Twitter.WordCount --master local /root/srinivas/test/wordcount.jar localhost 9999
 * */
 