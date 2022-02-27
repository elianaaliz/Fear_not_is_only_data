package io.keepcoding.spark.exercise.streaming



import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.Duration


object UserStreamingJob extends StreamingJob{
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._


  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val struct = StructType(Seq(
      StructField("bytes", LongType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("app", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false)
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      .select($"value.*")

  }

  def totalBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "1 minutes"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value")
      .withColumn("type",lit("antenna_bytes_total"))
  }

  def totalBytesByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "1 minutes"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"id", $"value")
      .withColumn("type",lit("id_bytes_total"))
  }

  def totalBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "1 minutes"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value")
      .withColumn("type",lit("app_bytes_total"))
  }

  // escribir a postgress
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }
  
  // esto es escribir en localstorage
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {


    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/spark-checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)

}
