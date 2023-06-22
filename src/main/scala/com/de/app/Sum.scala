package com.de.app

import org.apache.spark.sql.SparkSession


object Sum extends App{

    val spark = SparkSession.builder
      .appName("sum app")
      .master("local")
      .getOrCreate()

    println(globalVar)

    val rdd = spark.sparkContext.parallelize(Seq(4,7,9))
    val sum = rdd.reduce{ (a,b) => (a + b)}
    println("sum="+sum)
    spark.stop()

}

