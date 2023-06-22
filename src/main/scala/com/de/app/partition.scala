package com.de.app

import com.de.app.cdc.{schema_details, spark}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object partition extends App {

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df1 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    //.schema(schema_details)
    //.option("dateFormat", "yyyy/MM/dd")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\today.csv")
  /*
   df1.withColumn("country",lit("UK"))
     .coalesce(1)
     .write
     .partitionBy("country")
     .mode(saveMode = "append")
     .format("csv")
     .save("C:\\scalatest\\in-de-codepair-template-scala\\part\\")


     df1.coalesce(1)
       .write
       .partitionBy("date")
       .mode(saveMode = "append")
       .format("csv")
       .save("C:\\scalatest\\in-de-codepair-template-scala\\part\\")*/

  val df2 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\")

  println(df2.count())

  spark.stop()

}
