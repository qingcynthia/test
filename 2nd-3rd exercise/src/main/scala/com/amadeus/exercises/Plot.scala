package com.amadeus.exercises

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Plot {
  def main(args: Array[String]) {
    val sc = MySparkContext.createSparkContext
    val sqlContext = new SQLContext(sc)

    val searches = sqlContext.read
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .option("delimiter", "^")
       .option("inferSchema", "true")
       .option("mode", "DROPMALFORMED")
       .load("searches.csv.bz2")
    searches.registerTempTable("searchesTable")
    val df = sqlContext.sql("select Date, Destination from searchesTable")
    val resultAGP = monthlyCount(df, "AGP")
    println("AGP")
    resultAGP.foreach(println)

    val resultMAD = monthlyCount(df, "MAD")
    println("MAD")
    resultMAD.foreach(println)

    val resultBCN = monthlyCount(df, "BCN")
    println("BCN")
    resultBCN.foreach(println)
  }

  def monthlyCount(df: DataFrame, airport: String): RDD[(String, Long)] = {
    df.filter(col("Destination") === airport).map(r => (r.getString(0).substring(0, 7), 1L)).reduceByKey(_+_)
  }

  def createChart() {
  }
}
