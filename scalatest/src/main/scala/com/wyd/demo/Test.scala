package com.wyd.demo

import com.wyd.traittest.Fraction

object Test {

  val f:(Int,Int) => Int = (a,b) => a +b

  val f2 = (a:Int, b:Int) => a * b

  val f3 = (x:Int) => x*2

  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4,5,6)

    val fraction = Fraction(2,3)
    val fk = fraction.createf(1,2)

    val arrs = arr.map(fk)

    for(x <- arrs){
      println(x)
    }

  }

}
