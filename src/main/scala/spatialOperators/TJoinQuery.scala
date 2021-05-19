package spatialOperators

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.Polygon
import utils.distanceFunctions



class TJoinQuery {

  def pointPointRealtime(pointDataFrame: DataFrame, qPointDataFrame: DataFrame, qRadius: Double, SSession: SparkSession): DataFrame ={

    val projectedOrdStream = pointDataFrame
      .select(
        col("oID").as("id"),
        col("timestamp").as("timestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("x"),
        col("geometry.coordinates")(1).cast(DoubleType).as("y"),
        col("joinCol1").as("joinCol1")
      )

    val projectedQryStream = qPointDataFrame
      .select(
        col("oID").as("qID"),
        col("timestamp").as("qTimestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("qx"),
        col("geometry.coordinates")(1).cast(DoubleType).as("qy"),
        col("joinCol2").as("joinCol2")
      )

    import SSession.implicits._
    // Flatmap function: Generates replicated tuples with column names _1, _2, _3, _4, _5
    val replicatedQryDF = projectedQryStream.flatMap(row =>
      for{i <- 1 to 30} yield(row.getString(0) , row.getTimestamp(1), row.getDouble(2), row.getDouble(3), i.toString))

    val pointDataFrameWWatermark = projectedOrdStream.withWatermark("timestamp", "1 minute")
    val qryDataFrameWWatermark = replicatedQryDF.withWatermark("_2", "1 minute")

    val joinedDF = pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(
      //  """joinCol1 = joinCol2 AND qTimestamp >= timestamp - interval 1 second AND qTimestamp <= timestamp + interval 1 second"""))
      """joinCol1 = _5 AND timestamp >= _2 AND timestamp <= _2 + interval 1 second"""))


    return joinedDF.select(
      col("id"),
      col("x"),
      col("y"),
      col("_1"),
      col("_3"),
      col("_4")
    ).filter(e => distanceFunctions.pointPointEuclideanDistance(e.getDouble(1), e.getDouble(2), e.getDouble(4), e.getDouble(5)) < qRadius)

    //return projectedOrdStream

    /*
    val pointDataFrameWWatermark = projectedOrdStream.withWatermark("timestamp", "1 minute")
    val qryDataFrameWWatermark = replicatedQryDF.withWatermark("qTimestamp", "1 minute")

    // Since equality condition is mandatory, using dummy columns joinCol1 and joinCol2 to perform join
    val joinedDF = pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(
      //  """joinCol1 = joinCol2 AND qTimestamp >= timestamp - interval 1 second AND qTimestamp <= timestamp + interval 1 second"""))
      """joinCol1 = joinCol2 AND timestamp >= qTimestamp AND timestamp <= qTimestamp + interval 1 second"""))

    //val joinedDF = pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr("""joinCol1 = joinCol2"""))

    // Computing the distance using the joined streams
    return joinedDF.select(
      col("id"),
      col("x"),
      col("y"),
      col("qID"),
      col("qx"),
      col("qy")
    ).filter(e => distanceFunctions.pointPointEuclideanDistance(e.getDouble(1), e.getDouble(2), e.getDouble(4), e.getDouble(5)) < qRadius)
     */

    //return pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(
    //  """id = qID AND timestamp <= qTimestamp + interval 1 minute AND timestamp >= qTimestamp - interval 1 minute"""))

    //return pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(""" id = qID"""))

    // Spark does not support inequality stream-stream join
    //return pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(
    //  """timestamp <= qTimestamp + interval 1 minute AND timestamp >= qTimestamp - interval 1 minute """))
  }
}
