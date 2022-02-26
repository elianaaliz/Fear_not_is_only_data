# Fear not ... is only data
## _Big Data Processing Project_

El proyecto final se contruye siguiendo la arquitectura lambda para el procesamiento de datos recolectados desde antenas de telefonía movil.

## Arquitectura lambda

- Speed Layer: Capa de procesamiento en streaming. Computa resultados en tiempo real y baja latencia.
- Batch Layer: Capa de procesamiento por lotes. Computa resultados usando grande cantidades de datos, alta latencia.
- Serving Layer: Capa encargada de servir los datos, es nutrida por las dos capas anteriores.

## Fuente de Datos
En este proyecto vamos a trabajar con 2 fuentes de datos:
- Uso de datos de los dispositivos móviles.
- Base de datos con información de los usuarios.

### Uso de datos de los dispositivos móviles.

Esta fuente de datos, es una fuente de datos en tiempo real en formato JSON, las distintas antenas de nuestra red recolectaran la información de los dispositivos conectados y enviaran los mensajes con el siguiente schema:

| property   | description                     | data_type   |  example                                             |
|------------|---------------------------------|-------------|------------------------------------------------------|
| timestmap  | Marca de tiempo en segundos     | LONG        | `1600528288`                                         |
| id         | UUID del dispositivo movil      | STRING      | `"550e8400-e29b-41d4-a716-446655440000"`             |
| antenna_id | UUID de la antena               | STRING      | `"550e8400-e29b-41d4-a716-446655440000"`             |
| bytes      | Número de bytes transmitidos    | LONG        | `512`, `158871`                                      |
| app        | Aplicación utilizada            | STRING      | `"SKYPE"`, `"FACEBOOK"`, `"WHATSAPP"`, `"FACETIME"`  |

### Formato JSON

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "antenna_id": "550e8400-1234-1234-a716-446655440000", "app": "SKYPE", "bytes": 100}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "antenna_id": "550e8400-1234-1234-a716-446655440000", "app": "FACEBOOK", "bytes": 23411}
...
```

### Base de datos con información de los usuarios.

En concreto tendremos una tabla relacional con información sobre usuarios:

### Información de usuarios

| property   | description                            | data_type   |  example                                             |
|------------|----------------------------------------|-------------|------------------------------------------------------|
| id         | UUID del dispositivo movil             | TEXT        | `"550e8400-e29b-41d4-a716-446655440000"`             |
| name       | Nombre del usuario                     | TEXT        | `"Andres"`                                           |
| email      | Número de bytes transmitidos           | TEXT        | `andres@gmail.com`                                   |
| quota      | Número de bytes por hora permitidos    | BIGINT      | `10000000`                                           |


# Goal

En nuestra arquitectura lambda vamos a hacer distintos procesamientos, con la finalidad de obtener:

- Un servicio de analíticas de clientes.
- Un datalake de información histórica agregada.

## Speed Layer

### Un servicio de analíticas de clientes.

* Recolecta las métricas de los antenas y son almacenadas en Apache Kafka en tiempo real.
* Spark Structured Streaming, hace métricas agregadas cada 5 minutos y guarda en PostgreSQL.
    * Total de bytes recibidos por antena.
    * Total de bytes transmitidos por id de usuario. 
    * Total de bytes transmitidos por aplicación.
* Spark Structured Streaming, también enviara los datos en formato PARQUET a un almacenamiento de google cloud storage, particionado por AÑO, MES, DIA, HORA.

En primer lugar se crea y configura nuestro sistema de speed layer, para ellos vamos a crear una instancia en google compute engine, y vamos a configurarla para poder hacer funcionar un broker de Apache Kafka, donde se recibirán los mensajes en tiempo real de las fuentes datos móviles y BBDD de usuarios.

Ahora que ya tenemos los datos en Kafka, es el momento de crear nuestro job de Spark Structured Streaming para conseguir lás métricas y almacenar el histórico de datos.

Se ejecuta el job de spark dentro de DataProc/Local e indicar por argumento:
* La dirección del broker de kafka, es decir la dirección IP pública de nuestra instancía.
* La cadena de conexión JDBC para conectarse con GoogleSQL.
* La URI de Google Cloud Storage donde se almacenara el histórico de datos en parquet.

Hay que crear la tabla en postgresql antes de ejecutar el job de structuredStreaming. Esto se creará con el archivo **JdbcProvisioner.scala** que se encuentra en la carpeta **provisioner**

```scala
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")

            println("Creando la tabla bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")
            statement.execute("CREATE TABLE bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")

            println("Creando la tabla bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")
            statement.execute("CREATE TABLE bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")

            println("Creando la tabla user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP)")
            statement.execute("CREATE TABLE user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP)")


            println("Creando la tabla user_metadata(id TEXT, name TEXT, email TEXT, quota BIGINT)")
            statement.execute("CREATE TABLE IF NOT EXISTS user_metadata(id TEXT, name TEXT, email TEXT, quota BIGINT)")


            println("Dando de alta la información de usuarios")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
            statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")
```

## Resolviendo las Métricas

### Total de bytes recibidos por antena
```scala
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
````

### Total de bytes transmitidos por id de usuario
```scala
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
````

### Total de bytes transmitidos por aplicación
```scala
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
````

Podemos modelar todas las métricas agregadas de bytes se guardarán en una tabla como la siguiente:
```sql
CREATE TABLE bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
```

* **timestamp**: marca de tiempo format TimestampType en spark
* **id**: cualquier identificador dependiendo de la métrica podría ser: `id`, `antenna_id`, `app`
* **value**: valor total de bytes, aunque podría ser cualquier valor numerico: LongType o IntType
* **type**: nombre de la métrica por ejemplo: `app_bytes_total`, `antenna_bytes_total`

Este formato de tabla, nos permite guardar todas las métricas resultantes del job de structuredStreaming dentro de una misma tabla, es deber del job adaptar los datos para que cumplan con este esquema de salida.



## Batch Layer

En esta capa vamos trabajar con los datos que el job de structuredStreaming va creando en el storage. El job de batch (sparkSQL) deberá cargar estos datos filtrando por hora y calcular las métricas que hemos visto anteriormente:

### Un servicio de analíticas de clientes.
* Total de bytes recibidos por antena.
* Total de bytes transmitidos por mail de usuario.
* Total de bytes transmitidos por aplicación.
* Email de usuarios que han sobrepasado la cuota por hora.

Todas las métricas serán almacenadas en PostgreSQL.

Para calcular estas métricas usara los datos volcados por el job de structured streaming y necesitara acceder a la tabla de metadatos de usuario para descubrir los emails y las quotas de los usuarios. Los resultado pueden volcarse en unas tablas mediante conexión jdbc, que pueden tener unos schema como los siguientes:

```sql
CREATE TABLE bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
CREATE TABLE user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP);
```

## Resolviendo las Métricas

### Total de bytes recibidos por antena

```scala
def totalBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy($"antenna_id", window($"timestamp", "1 hour")) //cambiarlo 1h
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value")
      .withColumn("type",lit("antenna_bytes_total"))
  }
  ```

### Total de bytes transmitidos por mail de usuario

```scala
 def totalBytesByMail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"email".as("id"), $"value")
      .withColumn("type",lit("mail_bytes_total"))
  }
  ```

### Total de bytes transmitidos por aplicación

```scala
 def totalBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy($"app", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("value")
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value")
      .withColumn("type",lit("app_bytes_total"))
  }
  ```

### Email de usuarios que han sobrepasado la cuota por hora

```scala
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
      .filter($"d1.value" > $"d2.quota"//usuarios que superan su cuota  $"d2.quota"
      )
      .select($"d2.email".as("email"),
        $"d1.value".as("usage"),
        $"d2.quota".as("quota"),
        $"d1.timestamp".as("timestamp"))

  }
  ```
  
Para calcular estas métricas usara los datos volcados por el job de structured streaming y necesitara acceder a la tabla de metadatos de usuario para descubrir los emails y las quotas de los usuarios. Los resultado pueden volcarse en unas tablas mediante conexión jdbc, que pueden tener unos schema como los siguientes:

```sql
CREATE TABLE bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
CREATE TABLE user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP);
```

# (*)Parte opcional
## Serving Layer

La serving layer, sera un conjuntos de dashboard en superset que ataquen a la base de datos PostgreSQL y obtengan las métricas generadas por la speed layer y por la batch layer.

Una vez tenemos todos los datos en nuestra capa de servicios (PostgreSQL), podemos conectarnos a la base de datos y consultar los datos para la visulización de resultados.

Por ejemplo, a continuación se adjuntará dos capturas de imágenes de las consultas:

### Total de bytes transmitidos por aplicación


### Email de usuarios que han sobrepasado la cuota (limitado a 10)

![](../images/superset_create.png)
![](../images/superset_graph.png)


-------------------------------------
