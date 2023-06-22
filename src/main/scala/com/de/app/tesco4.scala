package com.de.app

object tesco4 extends App {

  /*Input:
    list(("a","aa",1),("b","bb",2),("c","cc",3))

  output:
    list(("a",1),("b",4),("c",9))
*/

  val a = List(("a", "aa", 1), ("b", "bb", 2), ("c", "cc", 3))

  val b = a.map(x => (x._1, x._3 * x._3))
  a.map { case (x, y, z) => (x, z * z) }
  b.foreach { case (first, squaredNumber) =>
    println(s"($first, $squaredNumber)")
  }


  /*  bookId     Sales
    1             Array[10,20,1]
    2             Array[2,3]
    3             Array[3]
    Output:
    1,31
    2,5
    3,3*/

  val ip = Map(
    1 -> Array(10, 20, 1),
    2 -> Array(2, 3),
    3 -> Array(3)
  )

  val op= ip.map{ case (x,y) =>
  val total = y.reduce(_+_)
    (x,total)
  }
  op.foreach{ case (x,y) => println(x,y)}

  //convert list to Map
val lst = List((1,"a"),(2,"b"))
  println(lst.toMap)

}

