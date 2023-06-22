package com.de.app

import org.apache.spark.sql.SparkSession

object bucketby extends App{

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  spark.sql("create database mydb")


  val df1 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\yesterday.csv")

  df1.write.format("csv")
    .bucketBy(2,"id").saveAsTable("mydb.bucket")
    //.save("C:\\scalatest\\in-de-codepair-template-scala\\bucket\\")

  spark.sql("select * from mydb.bucket").show()

  /*

    val df2 = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("C:\\scalatest\\in-de-codepair-template-scala\\today.csv")

    df2.write.format("csv")
      .bucketBy(2,"id")
      .save("C:\\scalatest\\in-de-codepair-template-scala\\bucket\\")
  */


  spark.stop()


}
