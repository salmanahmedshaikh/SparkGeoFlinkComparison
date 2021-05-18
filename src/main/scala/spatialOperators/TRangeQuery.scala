package spatialOperators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import utils.distanceFunctions
//import spatialObjects.Point
import utils.distanceFunctions.pointPointEuclideanDistance

class TRangeQuery {

  /*
  val getPoint: UserDefinedFunction = udf((x: Double, y: Double) => {

    val geofact = new GeometryFactory()
    val coord = new Coordinate(x,y)
    val point = geofact.createPoint(coord)

    point
  })
   */

  //val UDFGetP = udf[org.locationtech.jts.geom.Point, Double, Double](distanceFunctions.getPoint)

  /*
  def lowerRemoveAllWhitespace(s: String): String = {
    s.toLowerCase().replaceAll("\\s", "")
  }

  val lowerRemoveAllWhitespaceUDF = udf[String, String](lowerRemoveAllWhitespace)
   */

  def pointRangeRealtime(pointDataFrame: DataFrame, qPolygon: Polygon): DataFrame ={

    val qDistanceStream = pointDataFrame
      .select(
        col("oID").as("id"),
        col("timestamp").as("timestamp"),
        col("geometry.coordinates")(0).cast(DoubleType).as("x"),
        col("geometry.coordinates")(1).cast(DoubleType).as("y")
        //UDFGetP(col("geometry.coordinates")(0), col("geometry.coordinates")(1)).as("point")
      )
      //.filter(p => qPolygon.contains(p.get(4).asInstanceOf ))
      .filter(p => qPolygon.contains( distanceFunctions.getPoint(p.getDouble(2), p.getDouble(3)) ))



      return qDistanceStream
  }
}
