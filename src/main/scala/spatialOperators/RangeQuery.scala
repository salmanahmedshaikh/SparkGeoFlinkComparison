package spatialOperators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, window, pow, sqrt}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DoubleType
import spatialObjects.Point
import utils.distanceFunctions.pointPointEuclideanDistance

class RangeQuery() {

  def pointRangeRealtime(pointDataFrame: DataFrame, qPoint: Point, qRadius: Double): DataFrame ={

    val qDistanceStream = pointDataFrame
      .select(
        col("oID").as("id"),
        col("timestamp").as("timestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("x"),
        col("geometry.coordinates")(1).cast(DoubleType).as("y"),
        pointPointEuclideanDistance(col("geometry.coordinates")(0), col("geometry.coordinates")(1), qPoint.x, qPoint.y).as("qDistance")
      )
      .filter(p => p.getDouble(4) < qRadius) // p.getDouble(4) - the 5th variable of select

    return qDistanceStream
  }

  def pointRangeWindowed(pointDataFrame: DataFrame, qPoint: Point, qRadius: Double): DataFrame ={

    val qDistanceStream = pointDataFrame
      .select(
        col("oID").as("id"),
        col("timestamp").as("timestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("x"),
        col("geometry.coordinates")(1).cast(DoubleType).as("y"),
        pointPointEuclideanDistance(col("geometry.coordinates")(0), col("geometry.coordinates")(1), qPoint.x, qPoint.y).as("qDistance")
      )
      .filter(p => p.getDouble(4) < qRadius) // p.getDouble(4) - the 5th variable of select
      .groupBy(window(col("timestamp"),"5 seconds", "5 seconds"), col("id"), col("x"), col("y"), col("qDistance")).count()

    return qDistanceStream

    /*
    val windowedStream = pointDataFrame
      .groupBy(window(pointDataFrame.col("timestamp"),"5 seconds", "5 seconds"),
        col("geometry.coordinates")(0),
        col("geometry.coordinates")(1))
      .count()
     */

    //.filter(p => pointPointEuclideanDistance(p.getDouble(1), p.getDouble(2), qPoint.x, qPoint.y) < qRadius)
    //val filteredStream = qDistanceStream.filter(col("qDistance") < qRadius)


    //.filter(  (sqrt(col("x") - qPoint.x) * (col("x") - qPoint.x)  +  (col("y") - qPoint.y) * (col("y") - qPoint.y)) < qRadius)
    //.filter(  (sqrt( col("x") - qPoint.x ) * ( col("x") - qPoint.x )  +  (col("y") - qPoint.y) * (col("y") - qPoint.y)) < qRadius )
    //val filteredStream = qDistanceStream.filter(pointPointEuclideanDistance(col("x"), col("y"), qPoint.x, qPoint.y) < qRadius)



    //.filter( p => pointPointEuclideanDistance(col("x"), col("y"), qPoint.x, qPoint.y) < qRadius)
    //.filter( p => pointPointEuclideanDistance(qPoint.x, qPoint.y, qPoint.x, qPoint.y) < qRadius)
    //.filter( p => Math.sqrt(Math.pow((col("x") - qPoint.x),2) + Math.pow((col("x") - qPoint.y),2))  < qRadius)
    //.filter( p => sqrt(pow((col("x") - qPoint.x),2) + pow((col("x") - qPoint.y),2))  < qRadius)

    //castedStream.filter(pointPointEuclideanDistance(castedStream.col("x"), castedStream.col("y"), qPoint.x, qPoint.y) > qRadius)

    //.groupBy(window(pointDataFrame.col("timestamp"),"5 seconds", "5 seconds"),
    //  col("geometry"))

    /*
    val windowedStream = pointDataFrame
      .groupBy(window(pointDataFrame.col("timestamp"),"5 seconds", "5 seconds"))
    */
  }
}