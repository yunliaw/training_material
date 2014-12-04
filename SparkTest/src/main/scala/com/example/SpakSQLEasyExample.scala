package com.example

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

object SpakSQLEasyExample {

  // Define the schema using a case class.
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._

    val users = sqlContext.parquetFile("data/users.parquet")
    users.registerTempTable("users")
    sqlContext.sql("select * from users").collect().foreach(println)

  }
}
