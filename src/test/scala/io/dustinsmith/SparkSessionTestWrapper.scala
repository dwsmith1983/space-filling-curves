package io.dustinsmith

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName("spark test session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
