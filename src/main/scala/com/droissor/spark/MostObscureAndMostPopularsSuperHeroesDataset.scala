package com.droissor.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostObscureAndMostPopularsSuperHeroesDataset {

  private val NAMES_DATA_FILE_PATH = "data/Marvel-names.txt"
  private val CONNECTIONS_DATA_FILE_PATH = "data/Marvel-graph.txt"

  private val SUPER_HERO_NAME_SCHEMA = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("MostObscureSuperheroDataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val superHeroesNames = spark.read
      .schema(SUPER_HERO_NAME_SCHEMA)
      .option("sep", " ")
      .csv(NAMES_DATA_FILE_PATH)
      .as[SuperHeroNames]

    val superHeroesConnectionsGraph = spark.read
      .text(CONNECTIONS_DATA_FILE_PATH)
      .as[SuperHero]

    val connections = superHeroesConnectionsGraph
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("totalConnections"))

    val minConnections = connections.agg(min("totalConnections")).first().getLong(0)

    val mostObscureSuperHeroes = connections
      .filter(col("totalConnections") === minConnections)
      .join(superHeroesNames, "id")

    mostObscureSuperHeroes.show(mostObscureSuperHeroes.count().toInt)

    val mostPopularSuperHeroes = connections
      .join(superHeroesNames, "id")
      .sort(col("totalConnections").desc)

    mostPopularSuperHeroes.show()
  }
}