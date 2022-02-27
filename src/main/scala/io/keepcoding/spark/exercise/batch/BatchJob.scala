package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class UserMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def readBytesMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichUserWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def totalBytesByAntenna(dataFrame: DataFrame): DataFrame

  def totalBytesByMail(dataFrame: DataFrame): DataFrame

  def totalBytesByApp(dataFrame: DataFrame): DataFrame

  def userQuotaLimit(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, jdbcMetadata2Table, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val deviceDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataUserDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val metadataBytesDF = readBytesMetadata(jdbcUri, jdbcMetadata2Table, jdbcUser, jdbcPassword)
    val devicesMetadataDF = enrichUserWithMetadata(deviceDF, metadataUserDF).cache()
    val aggByAntennaDF = totalBytesByAntenna(devicesMetadataDF)
    val aggByAppDF = totalBytesByApp(devicesMetadataDF)
    val aggByMailDF = totalBytesByMail(devicesMetadataDF)
    val aggByQuotaLimitDF = userQuotaLimit(metadataBytesDF, metadataUserDF)

    writeToJdbc(aggByAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByAppDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByMailDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByQuotaLimitDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

    writeToStorage(deviceDF, storagePath)

    spark.close()
  }

}
