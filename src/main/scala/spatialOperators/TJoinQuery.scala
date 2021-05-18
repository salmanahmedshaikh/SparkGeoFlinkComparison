package spatialOperators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.Polygon
import utils.distanceFunctions

class TJoinQuery {

  def pointPointRealtime(pointDataFrame: DataFrame, qPointDataFrame: DataFrame, qRadius: Double): DataFrame ={


    val projectedOrdStream = pointDataFrame
      .select(
        col("oID").as("id"),
        col("timestamp").as("timestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("x"),
        col("geometry.coordinates")(1).cast(DoubleType).as("y"),
        col("joinCol1")
      )

    val projectedQryStream = qPointDataFrame
      .select(
        col("oID").as("qID"),
        col("timestamp").as("qTimestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("qx"),
        col("geometry.coordinates")(1).cast(DoubleType).as("qy"),
        col("joinCol2")
      )

    val pointDataFrameWWatermark = projectedOrdStream.withWatermark("timestamp", "1 minute")
    val qryDataFrameWWatermark = projectedQryStream.withWatermark("qTimestamp", "1 minute")

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

    //return pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(
    //  """id = qID AND timestamp <= qTimestamp + interval 1 minute AND timestamp >= qTimestamp - interval 1 minute"""))

    //return pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(""" id = qID"""))

    // Spark does not support inequality stream-stream join
    //return pointDataFrameWWatermark.join(qryDataFrameWWatermark, expr(
    //  """timestamp <= qTimestamp + interval 1 minute AND timestamp >= qTimestamp - interval 1 minute """))
  }
}
