package spatialOperators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DoubleType
import spatialObjects.Point
import utils.distanceFunctions.pointPointEuclideanDistance

class FilterQuery {

  def pointFilterRealtime(pointDataFrame: DataFrame, trajIDList: List[String]): DataFrame = {

    val qDistanceStream = pointDataFrame
      .select(
        col("oID").as("id"),
        col("timestamp").as("timestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("x"),
        col("geometry.coordinates")(1).cast(DoubleType).as("y")
      )
    .filter(col("id").isin(trajIDList:_*))

    return qDistanceStream
  }
}
