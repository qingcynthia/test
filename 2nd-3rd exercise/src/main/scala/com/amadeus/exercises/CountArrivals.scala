package com.amadeus.exercise

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object CountArrivals {
  def createSparkContext: SparkContext = {
    val sparkConf = new SparkConf().setAppName("Ex2").setMaster("local[*]")
    new SparkContext(sparkConf)
  }

  def main(args: Array[String]) {
    val sc = createSparkContext
    val sqlContext = new SQLContext(sc)

    val bookings = sqlContext.read
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .option("delimiter", "^")
       .option("inferSchema", "true")
       .option("mode", "DROPMALFORMED")
       .load("bookings.csv.bz2")
    bookings.registerTempTable("bookingsTable")
    val df = sqlContext.sql("select arr_port, pax from bookingsTable")
    df.groupBy("arr_port").agg(sum("pax").as("sumPax")).orderBy(desc("sumPax")).show(10)
    sc.stop()
  }
}
