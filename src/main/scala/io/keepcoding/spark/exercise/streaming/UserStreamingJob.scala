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

// metodo intocable
  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }
// cambiar la estructura json
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
// intocable cambiar nombre antenna por user
  /*override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }*/

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
// esto es escribir en local
  // en principio es para simular que se guarda en postgres y local en paralelo- pero va ser que no
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

  def main2(args: Array[String]): Unit = {
/*
    totalBytesbyUser(
      parserJsonData(
        readFromKafka("35.193.101.232:9092","devices")))
      .writeStream
      .format("console")
      .start()
      .awaitTermination()*/

    // creo que esto no es necesario para el proyecto -- sirve para probar si recibimos de postgres
    /*
    val metadata_user = readUserMetadata(s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "user_metadata",
      "postgres",
      "keepcoding"
    )//.show()*/


/*
    val future1 = writeToJdbc(
      totalBytesbyAntenna(
       parserJsonData(
        readFromKafka("35.202.44.187:9092","devices"))),
      s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")
*/
    /*
    val future2 = writeToJdbc(
      totalBytesbyUser(
        parserJsonData(
          readFromKafka("35.202.44.187:9092","devices"))),
      s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")
*/


/*
    val future3 = writeToJdbc(
      totalBytesbyApp(
        parserJsonData(
          readFromKafka("35.202.44.187:9092","devices"))),
      s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")

*/

    val future4 = writeToStorage(parserJsonData(readFromKafka("35.202.44.187:9092", "devices")), "/tmp/data-spark2")
    Await.result(Future.sequence(Seq(future4)), Duration.Inf)
    // descomentar este ultimo para entrega solo - ya que es una lista de futuros
    //Await.result(Future.sequence(Seq(future1, future2, future3, future4)), Duration.Inf)
  }

  def main(args: Array[String]): Unit = run(args)

}
