package utils

import org.apache.spark.sql.functions.{col, pow, sqrt, udf, window}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon}

object distanceFunctions {

  def pointPointEuclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double ={
    Math.sqrt(Math.pow((x2 - x1),2) + Math.pow((y2 - y1),2))
  }

  def pointPointEuclideanDistance(x1: Column, y1: Column, x2: Double, y2: Double): Column ={
    sqrt(pow((x1 - x2),2) + pow((y1 - y2),2))
  }

  def getPoint(x: Double, y: Double): org.locationtech.jts.geom.Point ={

    val geofact = new GeometryFactory()
    val coord = new Coordinate(x,y)
    val point = geofact.createPoint(coord)

    point
  }

  def polygonSetContainsPoint(polygonList: Array[Polygon], point: org.locationtech.jts.geom.Point): Boolean ={

    for(poly <- polygonList) {
      if (poly.contains(point)){
        return true
      }
    }
    return false
  }

}
