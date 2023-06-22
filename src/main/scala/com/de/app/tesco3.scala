package com.de.app

import org.apache.spark.sql.{DataFrame, SparkSession}

object tesco3 extends App {
  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import org.apache.spark.sql.functions._

  import spark.implicits._

  /* Question:6(spark)
  id,name,sal,date
  1,dilip,1000,01/01/2020
  1,dilip,2000,02/01/2020
  1,dilip,1000,03/01/2020
  1,dilip,2000,04/01/2020
  1,dilip,3000,05/01/2020
  1,dilip,1000,06/01/2020

  output
  id,name,sal,date,       trending
  1,dilip,1000,01/01/2020,  same
  1,dilip,2000,02/01/2020,  up
  1,dilip,1000,03/01/2020,  down
  1,dilip,2000,04/01/2020,  up
  1,dilip,3000,05/01/2020,  up
  1,dilip,1000,06/01/2020,  down
  */

  val data = Seq(
    (1, "dilip", 1000, "01/01/2020"),
    (1, "dilip", 2000, "02/01/2020"),
    (1, "dilip", 1000, "03/01/2020"),
    (1, "dilip", 2000, "04/01/2020"),
    (1, "dilip", 3000, "05/01/2020"),
    (1, "dilip", 1000, "06/01/2020")
  ).toDF("id", "name", "sal", "date")

  data.createOrReplaceTempView("t1")

  spark.sql("with cte1 as (select lag(sal, 1) over(partition by id order by date) as previoussal,* from t1) " +
    "select id,name,sal,date, case " +
    " when previoussal is null then 'same'" +
    " when sal = previoussal then 'same' when sal > previoussal then 'up' else 'down' end as trending" +
    " from cte1").show()

  import org.apache.spark.sql.expressions.Window
  val windowSpec = Window.partitionBy("id").orderBy("date")

  val previoussal = lag("sal",1, "same").over(windowSpec)
  data.withColumn("trending", when(col("sal")===previoussal,"same")
    .when(col("sal")>previoussal, "up").otherwise("down"))
    .withColumn("row", row_number() over(Window.orderBy("id"))).show()

  print(data.columns.length)

}
