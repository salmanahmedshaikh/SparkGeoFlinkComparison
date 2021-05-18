package spatialObjects

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}

class Point(val xc: Double, val yc: Double) extends Serializable {
    var x: Double = xc
    var y: Double = yc

    def this() = this(0, 0)

    val geofact = new GeometryFactory()
    val coord = new Coordinate(x,y)
    val point = geofact.createPoint(coord)
  }