package com.de.app
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object readcsv {

  def main(args:Array[String]) : Unit =
    {
      val spark = SparkSession.builder().appName("schema gen").master("local[2]").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      val emp_df = spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .option("samplingRatio","0.5")
        .load("C:\\scalatest\\in-de-codepair-template-scala\\employee.csv")
      emp_df.createOrReplaceTempView("employee")
      /*
      spark.sql("select row_number() over(order by sal desc) as rownum, * from employee").show()
      spark.sql("select rank() over(order by sal desc) as rnk, * from employee").show()
      spark.sql("select dense_rank() over(partition by sal order by sal desc) as dense_rnk, * from employee").show()
      spark.sql("select row_number() over(partition by sal order by sal desc) as rn, * from employee").show()
      spark.sql("select distinct sal from employee").show()*/
      //spark.sql("select name from employee where sal = (select sal from employee group by sal having count(sal) > 1)").show()

      val man_df = spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .option("samplingRatio","0.5")
        .load("C:\\scalatest\\in-de-codepair-template-scala\\manager.csv")

      man_df.createOrReplaceTempView("manager")

      spark.sql("select name from employee e join manager m on e.id=m.id where type='manager'").show()
      spark.stop()
    }
}
