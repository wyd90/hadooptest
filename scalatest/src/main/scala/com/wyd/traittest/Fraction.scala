package com.wyd.traittest

class Fraction(val a: Int, val b: Int) {
  def call(x: Int, y: Int, f1:(Int,Int) => Int, f2:(Int,Int) => Int): Int ={
    if(x >= 0) {
      f1(x,y)
    } else {
      f2(x,y)
    }
  }

  def createf(x: Int, y: Int):(Int) => Int ={
    val z = x + y
    val f:(Int) => Int = (x) => x + z
    f
  }
}

object Fraction{
  def apply(a: Int, b: Int):Fraction = new Fraction(a, b)

  def unapply(f: Fraction): Option[(Int, Int)] = Some((f.a,f.b))
}