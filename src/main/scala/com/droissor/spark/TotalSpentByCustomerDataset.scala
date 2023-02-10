package com.droissor.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object TotalSpentByCustomerDataset {

  private val DATA_FILE_PATH = "data/customer-orders.csv"

  private val CUSTOMER_ORDER_SCHEMA = new StructType()
    .add("customerId", IntegerType, nullable = false)
    .add("itemID", IntegerType, nullable = false)
    .add("price", DoubleType, nullable = false)

  case class CustomerOrder(customerId: Int, itemID: Int, price: Double)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("TotalSpentByCustomerDataset")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._
    val dataSet = sparkSession.read
      .schema(CUSTOMER_ORDER_SCHEMA)
      .csv(DATA_FILE_PATH)
      .as[CustomerOrder]

    val orderByCustomer = dataSet.groupBy(col("customerId"))

    val customerToAmountSpent = orderByCustomer.agg(round(sum("price"),2).alias("amountSpent"))

    customerToAmountSpent.sort("amountSpent").show(customerToAmountSpent.count().toInt)
  }
}
