package com.msr.Twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object fileStream {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]){
    
    val spark = SparkSession
    .builder()
    .appName("File_stream")
    .config("spark.sql.streaming.schemaInference","true")
    .master("local[*]")
    .getOrCreate()
    
    val mySchema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("year", IntegerType, true),
        StructField("rating", DoubleType, true),
        StructField("duration", IntegerType, true)
        
      )
    )
    import spark.implicits._
    val df = spark.readStream.schema(mySchema).csv("D:/Stream_file/")
   
    
      df.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()

      
    
  }
}