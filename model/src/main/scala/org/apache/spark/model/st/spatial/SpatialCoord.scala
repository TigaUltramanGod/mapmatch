package org.apache.spark.model.st.spatial

import org.locationtech.jts.geom.Coordinate

/**
 * two dimensional spatial coordinate containing meta info
 * use lon and lat to represent x and y
 */
class SpatialCoord(lon: Double, lat: Double)
  extends Coordinate(lon, lat) {

  def this(coordinate: Coordinate) {
    this(coordinate.getX, coordinate.getY)
  }


  /**
   * get longitude
   */
  def getLon: Double = x

  /**
   * get latitude
   */
  def getLat: Double = y

  /**
   * used for spatial coord copy
   *
   * @param other other SpatialCoord
   */
  override def setCoordinate(other: Coordinate): Unit = {
    super.setCoordinate(other)
  }
}
