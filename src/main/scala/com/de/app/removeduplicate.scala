package com.de.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType, StringType, StructField}

object removeduplicate extends App {

      val spark = SparkSession.builder
        .appName("dup app")
        .master("local[*]")
        .getOrCreate()

      val rdd = spark.sparkContext.parallelize(Seq((1,"jay"),(2,"raj")))
      rdd.foreach{ x=> println(x._1,x._2)}

    val columns = Seq("id","name")

     val df = spark.createDataFrame(List((1,"j"),(2,"r"))).toDF("id","name")
     val df1 = spark.createDataFrame(List((1,"j"),(2,"r"))).toDF(columns : _*)

  df.show()
  df1.show()

  val name_schema =
    StructType(
    StructField("ID", IntegerType, true) ::
      StructField("new_name",StringType, true) :: Nil)

  val schema1 = new StructType()
    .add("ID", IntegerType, nullable = false)
    .add("Name", StringType, nullable = false)

  val df3 = spark.createDataFrame(List((1,"j"),(2,"r"),(3,null))).toDF(name_schema.fields.map(_.name) : _*)
  df3.printSchema()
  df3.show()
  df3.na.fill("test").show()
  df3.na.drop().show()

  val df4 = spark.createDataFrame(List((1,"j"),(4,"r"))).toDF("id","name")//,(3,null)))//.toDF(schema1.fields.map(_.name) : _*)
  import spark.implicits._
  case class Person(id:Int, name:String)
  val ds : Dataset[Person] = df4.as[Person]
  ds.show()

 // println(name_schema.toDDL)
  spark.stop()
}

