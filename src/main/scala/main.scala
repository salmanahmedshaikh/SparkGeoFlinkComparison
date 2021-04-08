import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

object main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("testApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val kafka_bootstrap_servers = "localhost:9092"
    val kafka_topic_name = "TaxiDriveFewTuples"

    // data structure "TaxiDrive17MillionGeoJSON": {"geometry": {"coordinates": [116.50551, 39.92943], "type": "Point"}, "properties": {"oID": "1205", "timestamp": "2008-02-03 17:38:43"}, "type": "Feature"}
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()

    val inputDataSchema = spark
      .read
      .json("src/main/scala/jsonSchema.txt")
      .schema
    inputDataSchema.printTreeString()


    val df_cast = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    val df_val = df_cast.select(from_json(col("value"),inputDataSchema).as("inputTuple"), col("timestamp"))

    val df_col_val = df_val.select("inputTuple.geometry", "timestamp")

    df_col_val.printSchema()
    //df_col_val.show(10, false)

    val query = df_col_val.select(col("geometry.coordinates")(0)).writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()


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
