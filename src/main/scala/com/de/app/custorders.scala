package com.de.app
/*
scenario based question
a.	df1 custid custname custaddress
b.	df2 custname ordid orderlocation
c.	customerwise orders based on orderlocation.
d.	and how to improve performance here
*/

import org.apache.spark.sql.{SparkSession, Encoders, DataFrame}

object custorders extends App {

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  case class Person(id: Int, name: String, age: Int)

  import spark.implicits._
  val rdd1 = spark.sparkContext.parallelize(Seq((1,"jay","abc"),(2,"raj","def")))

  val df1 = spark.createDataFrame(rdd1).toDF("custid","custname","custaddress")

  val rdd2 = spark.sparkContext.parallelize(Seq(("jay",101,"ind"),("raj",102,"fra")))

  val df2 = spark.createDataFrame(rdd2).toDF("custname","ordid","orderlocation")

  df1.show()
  df2.show()
  import org.apache.spark.sql.functions._
  df1.join(df2,Seq("custname"),"inner")
    .groupBy("orderlocation","custname")
    .agg(collect_list("ordid"))
    .show()
  //select custname from t1 inner join t2 on t1.custname=t2.custname group by orderlocation

}
