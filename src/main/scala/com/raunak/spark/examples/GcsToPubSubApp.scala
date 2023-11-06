package com.raunak.spark.examples

import com.raunak.spark.examples.GcsToPubSubAProcess.{StreamToPubSub, readToDf}
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

  object GcsToPubSubApp {
    def main(args: Array[String]) {

      val jobArgs = new MakenewclassJobArgs(args)
      val confFile = jobArgs.conf.getOrElse("application.conf")
      val rowConfig = ConfigurationFactory.load(confFile)
      println("config file loaded")


      val spark = SparkSession
        .builder()
        .appName("gcsTopubsub")
        .getOrCreate()


      println("gcs to pubsub application started")

      val df = readToDf

      println("data read from gcs to df done")

      StreamToPubSub(df)


       val READ_FILE_PATH = "gs://qori-hadoop-test/film.csv"
       val WRITE_FILE_PATH = "gs://qori-hadoop-test/processed_films"



      spark.sparkContext.setLogLevel("WARN")



      df
        .coalesce(1)
        .write
        .option("header", true)
        .option("delimiter", "|")
        .mode("overwrite")
        .csv(WRITE_FILE_PATH)
    }


  }