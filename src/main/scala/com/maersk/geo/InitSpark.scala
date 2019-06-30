package com.maersk.geo

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

trait InitSpark extends Serializable {

  val spark: SparkSession = SparkSession.builder()
    .appName("Maersk Geolocalization with Spark Scala")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  def reader = spark.read
    .option("header", true)
    .option("delimiter", ",")
    .option("inferSchema", true)
    .option("mode", "DROPMALFORMED")

  def readerWithoutHeader = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")

  def close = {
    spark.close()
  }

  init

  private def init = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }

}
