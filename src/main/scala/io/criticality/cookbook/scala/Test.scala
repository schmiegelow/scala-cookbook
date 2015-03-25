package io.criticality.cookbook.scala

import scala.annotation.tailrec
import scala.collection.mutable

case class Foo(val bar : String)

object Test {
val bat = List("dtag", "Bla", "im", "google")
    bat.filter( f => Character.isUpperCase(f.charAt(0)) )
    val intList = List(2,7,9,1,6,5,8,2,4,6,2,9,8)
    val (big,small) = intList partition (_ > 5)
    val stringified = big map (s => s + ",") mkString

    def doSomething(i : Int) : Seq[Int] = {
      Seq(i, i *2)
    }

    val result = small flatMap (i => { Seq[Int](i, i*2)} ) map (s => s + ",")  mkString

    val range = 1 to 200

    def max(ints: List[Int]): Int = {
      @tailrec
      def maxAccum(ints: List[Int], theMax: Int): Int = {
        if (ints.isEmpty) {
          return theMax
        } else {
          val newMax = if (ints.head > theMax) ints.head else theMax
          maxAccum(ints.tail, newMax)
        }
      }
      maxAccum(ints, 0)
    }
    max(range toList)

    println(result)

    val names = Vector("Bob", "Fred", "Joe", "Julia", "Kim")
    names foreach( s => s + " Dobalina")
    for (name <- names if name.startsWith("J"))
    for (name <- names) println(name)
    val results = for (name <- names) yield (name takeRight 1)
    println(names)
    println(results)

    def toInt(in: String): Option[Int] = {
      try {
        Some(Integer.parseInt(in.trim))
      } catch {
        case e: NumberFormatException => None
      }
    }

    toInt("154564545") match {
      case Some(i) => println(i)
      case None => println("That didn't work.")
    }

    toInt("154564545").getOrElse(0)


    val x = mutable.MutableList(1, 2, 3, 4, 5)
    x += 6
    x ++= mutable.MutableList(7, 8, 9)

    val y = List(1,2,3,4)
    val z = 5 :: y
    println(y)
    println(z)
    val zz = y ::: List(5)
    println(zz)

    // create a map with initial elements
    var states = scala.collection.mutable.Map("AL" -> "Alabama", "AK" -> "Alaska")
    // add elements with +=
    states += ("AZ" -> "Arizona")
    states += ("CO" -> "Colorado", "KY" -> "Kentucky")
    // remove elements with -=
    states -= "KY"
    // update elements by reassigning them
    states("AK") = "Alaska, The Big State"

}