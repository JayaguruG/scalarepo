package com.de.app

import org.apache.spark.sql.SparkSession
object tesco extends App {

  val str1:String = "1 2 3 4 5 5 6 7 8 9 10 10"
  val str2:String = "1 2 3 4 5 5 11"
  val str3 = str1.split(" ")
  for (s <- str3)
  {
   // print(s)
  }
 // str1.split(" ").map(x => x.toInt*2).foreach(println)
  val n1 = str1.split(" ").map(x => x.toInt)
  val n2 = str2.split(" ").map(x => x.toInt).toSet
  val uniq = n1.filterNot(n2)
  //println(uniq.mkString(" "))

  val n3 = str1.split(" ").toSet
  val n4 = str2.split(" ").toSet

  println(n3.filterNot(n4).toList.sorted)
  println(n3.diff(n4).toList.sorted.mkString(" "))

  val L1 = List(("A","p1",20), ("B","p2",30))
  val L2 = List(("A","p1",20), ("c","p3",35))

//output:
  //("A","p1",20,100),("B","p2",30,"not in L2")
  val result = L1.map {
    case (a, b, c) =>
      L2.find(x => x._1 == a && x._2 == b) match {
        case Some((_, _, d)) => (a, b, c, d)
        case None            => (a, b, c, "not in L2")
      }
  }
  result.foreach(println)

  val input = "1,p1,p2,p3,p4\n2,p1,p2\n3,p1,p2,p3,p4,p5\n4,p1,p2,p3,p4,p5,p5,p6\n5,p1,p2,p3,p4,p5,p6,p7,p8"
  val lines = input.split("\n")
  //lines.foreach(println)
  lines.map(line => line.split(",").mkString(" ")).foreach(println)
  val maxCount = lines
    .map(line => line.split(",").count(_.contains("p")))
    .max

  val result1 = lines
    .flatMap(line => {
      val elements = line.split(",")
      val key = elements.head
      val count = elements.count(_.contains("p"))
      if (count == maxCount) Some(key,count) else None
    })
   // .distinct

  println(result1.mkString(","))

}
