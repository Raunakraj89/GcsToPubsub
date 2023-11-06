package com.raunak.spark.examples

import org.apache.spark


object GcsToPubSubAProcess  {


  def readToDf:[Unit] = {
    var df = spark.read
      .option("header", true)
      .option("delimiter", ";")
      .csv(READ_FILE_PATH)

    df.show(truncate = false, numRows = 20)

     }




  def StreamToPubSub()
}