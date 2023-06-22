package com.de.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, DateType}

object cdc extends App {

  val spark = SparkSession.builder().appName("schema gen").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val schema_details =
    StructType(
      StructField("id", IntegerType, false) ::
        StructField("name", StringType, true) ::
        StructField("sal", IntegerType, true) ::
        StructField("dt", DateType, true) :: Nil)

  val df1 = spark.read.format("csv")
    .option("header","true")
    //.option("inferSchema", "true")
    .schema(schema_details)
    .option("dateFormat", "yyyy/MM/dd")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\yesterday.csv")

  //df1.printSchema()
  //df1.show()

  val df2 = spark.read.format("csv")
    .option("header","true")
    .schema(schema_details)
    .option("dateFormat", "yyyy/MM/dd")
    .load("C:\\scalatest\\in-de-codepair-template-scala\\today.csv")

  //df2.show()

  df1.createOrReplaceTempView("old_tbl")
  df2.createOrReplaceTempView("new_tbl")

  //spark.sql("select * from old_tbl o left join new_tbl n on o.id=n.id " +
   // "where s.id not in (select id from new_tbl) union select * from new_tbl")

  spark.sql("select * from old_tbl where id not in " +
    "(select id from new_tbl) union select * from new_tbl")

  spark.sql("select o.* from old_tbl o left join new_tbl n " +
    "on o.id = n.id where n.id is null union select * from new_tbl")

 /* df3.coalesce(1)
    .write
    .mode(saveMode = "overwrite")
    .format("csv")
    .save("C:\\scalatest\\in-de-codepair-template-scala\\latest.csv")
*/

  df1.union(df2).createOrReplaceTempView("tbl")
  val df3 = spark.sql("select * from " +
    "(select row_number() over(partition by id order by dt desc) as rn, * from tbl) " +
    "tmp" +
    " where rn=1")
  df3.show()
  spark.stop()
}

