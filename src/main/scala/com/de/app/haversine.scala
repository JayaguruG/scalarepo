package com.de.app
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object haversine extends App{

  val spark = SparkSession.builder().appName("schema gen")
    .master("local[2]")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val distanceUDF = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val earthRadius = 6371 // Radius of the Earth in kilometers

    val dLat = Math.toRadians(lat2 - lat1)
    val dLon = Math.toRadians(lon2 - lon1)
    println("dlat, dlon:", dLat , dLon)

    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    earthRadius * c
  })

  val df = spark.createDataFrame(Seq(
    (1, 40.7128, -74.0060, 34.0522, -118.2437), // New York to Los Angeles
    (2, 51.5074, -0.1278, 48.8566, 2.3522) // London to Paris
  )).toDF("id", "lat1", "lon1", "lat2", "lon2")

  val result = df.withColumn("distance", distanceUDF(col("lat1"), col("lon1"), col("lat2"), col("lon2")))
  result.show()


}
