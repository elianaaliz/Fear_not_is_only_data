package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class UserMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def totalBytesByAntenna(dataFrame: DataFrame): DataFrame

  def totalBytesByUser(dataFrame: DataFrame): DataFrame

  def totalBytesByApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val devicesDF = parserJsonData(kafkaDF)
    val aggByAntenna = totalBytesByAntenna(devicesDF)
    val aggByUser = totalBytesByUser(devicesDF)
    val aggByApp = totalBytesByApp(devicesDF)
    val aggFuture1 = writeToJdbc(aggByAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFuture2 = writeToJdbc(aggByUser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFuture3 = writeToJdbc(aggByApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val storageFuture = writeToStorage(devicesDF, storagePath)

    Await.result(Future.sequence(Seq(aggFuture1, aggFuture2, aggFuture3, storageFuture)), Duration.Inf)

    spark.close()
  }

}
