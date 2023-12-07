package ch1

import org.apache.spark.sql.SparkSession

object App1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("our-first-one")
      .master("local")
      .getOrCreate()
  }

}
