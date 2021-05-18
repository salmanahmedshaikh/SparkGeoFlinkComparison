import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, lit, to_timestamp, unix_timestamp, window}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import spatialObjects.Point
import spatialOperators.{FilterQuery, RangeQuery, TJoinQuery, TRangeQuery}

object main {

  def main(args: Array[String]): Unit = {

    val maxOffsetsPerTrigger = "100000"
    val numThreads = "30"
    val triggerDuration = "500 milliseconds" // 500 milliseconds 5 seconds 10 minutes


    val spark = SparkSession.builder()
      .appName("testApp")
      .master("local[" + numThreads +"]")
      .config("spark.cores.max", numThreads)
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    /*
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("First Application")
     */

    //val sc = spark.sparkContext
    //val ssc = new StreamingContext(sc, Seconds(1))
    //val sc = ssc.sparkContext
    //sc.setLogLevel("OFF")


    //val kafka_bootstrap_servers = "localhost:9092"
    val kafka_bootstrap_servers = "150.82.97.204:9092"
    val query_kafka_topic_name = "TaxiDriveGeoJSON_Live"
    //val query_kafka_topic_name = "TaxiDrive17MillionGeoJSON"
    val kafka_topic_name = "TaxiDrive17MillionGeoJSON"

    // data structure "TaxiDrive17MillionGeoJSON": {"geometry": {"coordinates": [116.50551, 39.92943], "type": "Point"}, "properties": {"oID": "1205", "timestamp": "2008-02-03 17:38:43"}, "type": "Feature"}

    val inputStreamDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      //.option("startingOffsets", "latest")
      .load()
      .select(col("value").cast("string").alias("value"))
    //inputStreamDf.printSchema()

    val queryStreamDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", query_kafka_topic_name)
      //.option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .option("startingOffsets", "latest")
      .load()
      .select(col("value").cast("string").alias("value"))

    //inputDataSchema.printTreeString()
    val inputDataSchema = spark
      .read
      .json("src/main/scala/jsonSchema.txt")
      .schema

    // Ordinary stream
    val df_val = inputStreamDf.select(from_json(col("value"),inputDataSchema).as("inputTuple"))
    val df_col_val = df_val.select(col("inputTuple.properties.oID"), col("inputTuple.geometry"), lit("1").as("joinCol1")).withColumn("timestamp", current_timestamp())
    //val df_col_val = df_val.select(col("inputTuple.properties.oID"), col("inputTuple.geometry"), col("inputTuple.properties.timestamp").cast(TimestampType)) // .withColumn("timestamp2", to_timestamp(col("timestamp"),"yyyy-MM-dd HH:mm:ss"))

    // Query stream
    val df_query = queryStreamDf.select(from_json(col("value"),inputDataSchema).as("inputTuple"))
    val df_col_val_query = df_query.select(col("inputTuple.properties.oID"), col("inputTuple.geometry"), lit("1").as("joinCol2"), col("inputTuple.properties.timestamp").cast(TimestampType))


    //val df_cast = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    //val df_col_val = df_val.select("inputTuple.geometry", "inputTuple.properties.timestamp")
    //val df_col_val = df_val.select("inputTuple.geometry", "inputTuple.properties.timestamp").withColumn("timestamp2", to_timestamp(col("timestamp"),"yyyy-MM-dd HH:mm:ss"))

    //val df_col_val = df_val.select("inputTuple.geometry", "inputTuple.properties.timestamp").withColumn("timestamp", unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
    //df_col_val.printSchema()
    //df_col_val.show(10, false)


    val queryPoint = new Point(117.586968, 39.943898)
    val queryRadius = 10

    // TJoin Query
    val tJoinQuery = new TJoinQuery
    val queryOutput = tJoinQuery.pointPointRealtime(df_col_val, df_col_val_query, queryRadius)


    // filter query
    //val pointFilterQuery = new FilterQuery
    //val trajIDs: List[String] = List("3946", "7703")
    //val queryOutput = pointFilterQuery.pointFilterRealtime(df_col_val, trajIDs)

    // range query
    // val pointRangeQuery = new RangeQuery
    // val queryOutput = pointRangeQuery.pointRangeRealtime(df_col_val, queryPoint, queryRadius)

    /*
    // TRangeQuery
    val tRangeQuery = new TRangeQuery
    val geofact = new GeometryFactory()
    val coordinatesArray = new Array[Coordinate](5)

    coordinatesArray(0) = new Coordinate(115.50000, 39.60000)
    coordinatesArray(1) = new Coordinate(117.60000, 39.60000)
    coordinatesArray(2) = new Coordinate(117.60000, 41.10000)
    coordinatesArray(3) = new Coordinate(115.50000, 41.10000)
    coordinatesArray(4) = new Coordinate(115.50000, 39.60000)

    val qPoly = geofact.createPolygon(coordinatesArray)
    val queryOutput = tRangeQuery.pointRangeRealtime(df_col_val, qPoly)
     */


    queryOutput
      .writeStream
      .format("console")
      .outputMode("append") // append, update, complete
      .trigger(Trigger.ProcessingTime(triggerDuration))
      .option("truncate", "false")
      .start()
      .awaitTermination()


  /*
    val query = df_col_val.select(col("geometry.coordinates")(0)).writeStream
      .outputMode("append")
      .format("console")
      .start()
     */

    //val ts = to_timestamp("timestamp", "")
    // Group the data by window and word and compute the count of each group


    /*
    df_col_val.withColumn("timestamp",
      to_timestamp(col("timestamp_"),"yyyy-MM-dd HH:mm:ss"))
      .show(false)
     */

    /*
    //inputStreamDf
    val windowedStream = df_col_val
      //.withColumn("timestamp", to_timestamp(col("timestamp"),"yyyy-MM-dd HH:mm:ss"))
      //.withColumn("timestamp", unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
      //.withColumn("timestamp",  to_timestamp("timestamp", ""))
      //.withColumn("timestamp", unix_timestamp(col("timestamp"), "MM/dd/yyyy hh:mm:ss aa")
      //.withWatermark("timestamp", "5 seconds")
      //.groupBy(window(df_col_val.col("timestamp"), "5 seconds", "5 seconds")).count()
      .groupBy(window(df_col_val.col("timestamp"), "5 seconds", "5 seconds"), col("geometry.coordinates")(0)).count()

    windowedStream
      .writeStream
      .format("console")
      .outputMode("update") // append, update, complete
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("truncate", "false")
      .start()
      .awaitTermination()

     */

    //windowedCounts.awaitTermination()


    /*
    val df_schema = StructType()
      .add

    val json_schema = spark.read.


    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

     */

    //df.collect().foreach(println)

    /*
    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

     */

    /*
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").foreach()
      .as[(String, String)]().foreach()

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

     */

    //val rdd1 = sc.makeRDD(Array(1,2,3))
    //rdd1.collect().foreach(println)

  }

}
