package com.droissor.spark

import org.apache.spark.SparkContext

object AmountSpentCount {

  val CUSTOMER_ID_FIELD_POSITION = 0
  val PRICE_FIELD_POSITION = 2
  val FILE_DELIMITER = ","

  def main(args: Array[String]): Unit = {

    val sparkContext = new SparkContext("local[*]", "AmountSpentCount")

    /** Data Structure is : CustomerID, ItemID, Price */
    val input = sparkContext.textFile("data/customer-orders.csv")

    val customerToPriceSpent = input.map(extractCustomerToPriceSpent)

    val customerTotalSpent = customerToPriceSpent.reduceByKey((firstPrice, secondPrice) => firstPrice + secondPrice)

    val customerTotalSpentSortedDesc = customerTotalSpent.sortBy(_._2, ascending = false)

    customerTotalSpentSortedDesc.collect().foreach(println)

  }

  def extractCustomerToPriceSpent(line: String): (Int, Float) = {
    val fields = line.split(FILE_DELIMITER)
    (fields(CUSTOMER_ID_FIELD_POSITION).toInt, fields(PRICE_FIELD_POSITION).toFloat)
  }
}
