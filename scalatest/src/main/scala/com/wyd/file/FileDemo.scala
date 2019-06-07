package com.wyd.file

import scala.io.Source

object FileDemo {

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("/Users/wangyadi/Documents/a.xml")
    val lines: Iterator[String] = source.getLines()

    for(line <- lines){
      println(line)
    }
  }

}
