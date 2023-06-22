package com.de.app

import org.apache.spark.sql.{SparkSession,DataFrame}

object tesco2 extends App {
  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import org.apache.spark.sql.functions._

  import spark.implicits._

  val data = Seq(
    ("A","P3",7),
    ("A","P1",2),
    ("A","P2",9),
  ("B","P2",7),
  ("B","P1",10),
  ("B","P3",8),
  ("B","P1",1),
  ("C","P2",8),
  ("C","P1",1),
  ("D","P2",9),
    ("E","P2",8)
  )

  val df = data.toDF("id", "prod", "qty")
  val maxqtydf = df.groupBy("id").agg(max("qty").as("maxqty"))//.select("id","maxqty")

  df.createOrReplaceTempView("t1")
  maxqtydf.createOrReplaceTempView("t2")

  spark.sql("select t1.* from t1 join t2 on t1.id = t2.id and t1.qty = t2.maxqty").show()

  df.join(maxqtydf,Seq("id", "qty")).select("id","prod","qty").show()

}
