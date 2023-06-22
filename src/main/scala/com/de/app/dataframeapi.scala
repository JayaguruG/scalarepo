package com.de.app

import org.apache.spark.sql.{SparkSession,DataFrame}

object dataframeapi extends App {

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df1 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\today.csv")

  import org.apache.spark.sql.functions._

/*  df1.select("id","name")
    .withColumn("country",lit("UK")).show()
  df1.select("*").filter("id='102'").drop("id")
    .withColumnRenamed("sal","salary").show() */


  df1.groupBy("date").sum("sal").as("total sal").show()
  df1.groupBy("date").count().show()




}

