package com.example

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkStreamingExample {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("./checkpoint")

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.updateStateByKey[Int](updateFunc)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
