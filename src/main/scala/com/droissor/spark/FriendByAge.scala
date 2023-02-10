package com.droissor.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, round}

object FriendByAge {

  private val DATA_FILE_PATH = "data/fakefriends.csv"

  case class FakeFriend(id: Int, name: String, age: Int, friends: Long)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("FriendByAge")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._
    val dataSet = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(DATA_FILE_PATH)
      .as[FakeFriend]

    val friendGroupedByAge = dataSet.groupBy(col("age"))

    val averageFriendsByAge = friendGroupedByAge.agg(round(avg("friends"), 2).alias("averageFriends"))

    averageFriendsByAge.sort("age").show()
  }
}
