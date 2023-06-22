package com.de.app
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object caseStmt extends App{

  case class OtherDetails(age:Int, address: String)
  case class PersonalInfo(id:Int, name: String, otherDetails: OtherDetails)

  val spark = SparkSession.builder().appName("schema gen").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val ds = Seq(PersonalInfo(1,"jay",OtherDetails(30,"PY"))).toDS()
  ds.printSchema()

  println("Using Encoders")
  import org.apache.spark.sql.Encoders

  val cc = Encoders.product[PersonalInfo].schema
  cc.printTreeString()

  println("Using Scala Reflection")
  import org.apache.spark.sql.catalyst.ScalaReflection
  ScalaReflection.schemaFor[PersonalInfo].dataType.asInstanceOf[StructType].printTreeString()


  spark.stop()
}
