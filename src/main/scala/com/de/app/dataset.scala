package com.de.app
import org.apache.spark.sql.SparkSession

object dataset extends App{

  val spark = SparkSession.builder
    .appName("dup app")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  case class Person(id:Int, name:String)

  val ds = spark.createDataset(List((1,"j"),(4,"ds"),(3,null))).toDF("id","name").as[Person]
  ds.show()

  case class InnerClass(age:Int, address: String)
  case class OuterClass(id:Int, name: String, innerClass: InnerClass)

  val outerObj = OuterClass(1,"jay",InnerClass(30,"PY"))
  println(outerObj)

  spark.stop()

  //https://sparkbyexamples.com/spark/convert-case-class-to-spark-schema/
}
