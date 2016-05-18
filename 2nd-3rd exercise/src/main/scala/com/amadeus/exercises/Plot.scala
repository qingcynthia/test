package com.amadeus.exercises

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.jfree.data.category.DefaultCategoryDataset
import scalax.chart.module.ChartFactories
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Plot {
  def createSparkContext: SparkContext = {
    val sparkConf = new SparkConf().setAppName("Ex2").setMaster("local[*]")
    new SparkContext(sparkConf)
  }

  def main(args: Array[String]) {
    val sc = createSparkContext
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
    val resultAGP = monthlyCount(df, "AGP").collect
    resultAGP.foreach(println)

    val resultMAD = monthlyCount(df, "MAD").collect
    resultMAD.foreach(println)

    val resultBCN = monthlyCount(df, "BCN").collect
    resultBCN.foreach(println)
    createChart(resultAGP, resultMAD, resultBCN)
  }

  def monthlyCount(df: DataFrame, airport: String): RDD[(String, Long)] = {
    df.filter(col("Destination") === airport).map(r => (r.getString(0).substring(0, 7), 1L)).reduceByKey(_+_)
  }

  def createChart(resultAGP: Array[(String, Long)], resultMAD: Array[(String, Long)], resultBCN: Array[(String, Long)]) {
    val ds = new DefaultCategoryDataset
    resultAGP.foreach{ r =>
      ds.addValue(r._2, "AGP", r._1)
    }
    resultMAD.foreach{ r =>
      ds.addValue(r._2, "MAD", r._1)
    }
    resultBCN.foreach{ r =>
      ds.addValue(r._2, "BCN", r._1)
    }

    val chart = ChartFactories.BarChart(ds)
    chart.show()
  }
}
