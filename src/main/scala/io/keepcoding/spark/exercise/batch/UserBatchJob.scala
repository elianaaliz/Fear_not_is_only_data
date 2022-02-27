package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object UserBatchJob extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._
  // intocable
  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where(
        $"year" === lit(filterDate.getYear) &&
          $"month" === lit(filterDate.getMonthValue) &&
          $"day" === lit(filterDate.getDayOfMonth) &&
          $"hour" === lit(filterDate.getHour)
      )
  }
  // intocable -- aunque cambiar nombre de la funcion Antenna por user*
  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def readBytesMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

// creo que esto tendre que cambiar , pero lo necesitare para combinar tablas e informacio del kafka**
  override def enrichUserWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("devices")  // devices seria lo que pone en main*
      .join(
        metadataDF.as("user_metadata"),
        $"devices.id" === $"user_metadata.id"
      ).drop($"devices.id")
  }


  // Total de bytes recibidos por antena.

  def totalBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy($"antenna_id", window($"timestamp", "1 hour")) //cambiarlo 1h
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value")
      .withColumn("type",lit("antenna_bytes_total"))
  }

  // Total de bytes transmitidos por mail de usuario.
  // aqui creo que necesitare hacer un join de lo que pasa kafka y la base de datos o solo bbdd??

  // LE HE AGREADO EL ID DE USUARIO AL RPINCIPIO
  def totalBytesByMail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"email".as("id"), $"value")
      .withColumn("type",lit("mail_bytes_total"))
  }

  // Total de bytes transmitidos por aplicación.
  def totalBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy($"app", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value")
      .withColumn("type",lit("app_bytes_total"))
  }


  // Email de usuarios que han sobrepasado la cuota por hora.
  def userQuotaLimit(dataFrameByte: DataFrame, dataFrameUser: DataFrame): DataFrame = {
    val d1 = dataFrameByte
      .filter($"type" === lit("mail_bytes_total"))
      .select($"timestamp", $"id", $"value")
      .as("d1")
      .cache()

    val d2 = dataFrameUser
      .select($"id", $"email", $"quota")
      .as("d2")
      .cache()

    d1
      .join(d2,
        $"d1.id" === $"d2.email"
      )
      .drop($"d1.id")
      .filter($"d1.value" > $"d2.quota"//usuarios que superan su cuota  $"b.quota"
      )
      .select($"d2.email".as("email"),
        $"d1.value".as("usage"),
        $"d2.quota".as("quota"),
        $"d1.timestamp".as("timestamp"))

  }

    // intocable
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
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

  //// intocable
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  def main2(args: Array[String]): Unit ={

    // devices = readFromStorage("tmp/data-spark") viene de este

    val rawdf = readFromStorage("/tmp/data-spark2", OffsetDateTime.parse("2022-02-24T17:00:00Z"))
    //rawdf.show()
    //Thread.sleep(10000)

    // Para leer de la base de datos antes de hacer el join de ambos
    val userMetadata = readUserMetadata(s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "user_metadata",
      "postgres",
      "keepcoding"
    )

    val bytesMetadata = readUserMetadata(s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding"
    )


    val enrichDF = enrichUserWithMetadata(rawdf, userMetadata).cache() // aqui en este caso con nuestro proyecto

    //enrichDF.show()
    //Thread.sleep(10000)


    //val pruebadf = prueba1(metadataDF2)

    //pruebadf.show()

    //Thread.sleep(10000)

    //val uno = totalBytesMailperUser(enrichDF)
    //uno.show()
    //Thread.sleep(10000)

    //uso de las dos BBDD user_metadata y bytes_hourly
    val chungo = userQuotaLimit(bytesMetadata, userMetadata)
    chungo.show()

    Thread.sleep(10000)

    //xxxxxxxxxxxxxxxxxxxxxxxxxxx
    // para verificar mas dejar que kafka guarde mas en el parquet .. para q tenga mas datos
    //seguiria lo siguiente
    /*writeToJdbc(totalBytesMailperUser(enrichDF),
      s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")*/

    //totalBytesbyApp

    /*
    writeToJdbc(totalBytesbyApp(enrichDF),
      s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")


     */

    /*
    writeToJdbc(totalBytesbyAntenna(enrichDF),
      s"jdbc:postgresql://34.134.76.186:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")
    Thread.sleep(10000)
    */

  }

  def main(args: Array[String]): Unit = run(args)
}
