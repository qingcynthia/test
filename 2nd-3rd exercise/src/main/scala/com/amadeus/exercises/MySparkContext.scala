package com.amadeus.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MySparkContext {
  def createSparkContext: SparkContext = {
    val sparkConf = new SparkConf().setAppName("Ex2").setMaster("local[*]")
    new SparkContext(sparkConf)
  }
}
