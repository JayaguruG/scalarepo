package com.de.app

import org.apache.spark.sql.{SparkSession,DataFrame}

object tesco1 extends App {

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import org.apache.spark.sql.functions._

  import spark.implicits._

  import org.apache.spark.sql.functions._

  // Create the input DataFrame
  val data = Seq(
    ("A", 1, 6, 7),
    ("B", 2, 7, 6),
    ("C", 3, 8, 5),
    ("D", 4, 9, 4),
    ("E", 5, 8, 3)
  )
  val df = data.toDF("storeid", "p1", "p2", "p3")

  // Reshape the data using Spark's built-in functions
  val result = df.select(
    $"storeid",
    when($"p1" > $"p2" && $"p1" > $"p3", "p1")
      .when($"p2" > $"p1" && $"p2" > $"p3", "p2")
      .otherwise("p3")
      .as("product"),
    greatest($"p1", $"p2", $"p3").as("qty")
  )

  result.show()

  // Find the product with max quantity for each store
  val maxProduct = result.join(
    result.groupBy($"storeid").agg(max($"qty").as("max_qty")),
    Seq("storeid")
  ).filter($"qty" === $"max_qty")
    .select("storeid", "product", "qty")

  // Display the result
  maxProduct.show()

}
