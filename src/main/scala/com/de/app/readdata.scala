package com.de.app

import org.apache.spark.sql.SparkSession

object readdata extends App {

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df1 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\testdata\\cust.csv")

  df1.show()

  val df2 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\testdata\\details.csv")

  df2.show()
import org.apache.spark.sql.functions._
  import spark.implicits._
  val df3 = df1.join(df2, Seq("customer_name"))
    .select("customer_id","notification_emails")
    .groupBy("customer_id")
    .agg(collect_list("notification_emails")).show()

  df1.createOrReplaceTempView("t1")
  df2.createOrReplaceTempView("t2")

  spark.sql("select customer_id, collect_list(notification_emails) from t1 inner join t2 on" +
    " t1.customer_name=t2.customer_name group by customer_id").show()
}
