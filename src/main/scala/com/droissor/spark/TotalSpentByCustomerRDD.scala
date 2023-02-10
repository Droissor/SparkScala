package com.droissor.spark

import org.apache.spark.SparkContext

object TotalSpentByCustomerRDD {

  private val DATA_FILE_PATH = "data/customer-orders.csv"
  private val CUSTOMER_ID_FIELD_POSITION = 0
  private val PRICE_FIELD_POSITION = 2
  private val FILE_DELIMITER = ","

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]", "AmountSpentCount")

    /** Data Structure is : CustomerID, ItemID, Price */
    val input = sparkContext.textFile(DATA_FILE_PATH)

    val customerToPriceSpent = input.map(extractCustomerToPriceSpent)

    val customerToTotalSpent = customerToPriceSpent.reduceByKey((firstPrice, secondPrice) => firstPrice + secondPrice)

    val customerToTotalSpentSortedByTotalSpentDesc = customerToTotalSpent.sortBy(_._2, ascending = false)

    customerToTotalSpentSortedByTotalSpentDesc.collect().foreach(println)

  }

  private def extractCustomerToPriceSpent(line: String): (Int, Float) = {
    val fields = line.split(FILE_DELIMITER)
    (fields(CUSTOMER_ID_FIELD_POSITION).toInt, fields(PRICE_FIELD_POSITION).toFloat)
  }
}
