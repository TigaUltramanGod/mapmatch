package org.apache.spark.model.st.spatial

import org.locationtech.jts.geom.impl.CoordinateArraySequence
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}

class SpatialPoint(spatialCoord: SpatialCoord, factory: GeometryFactory)
  extends Point(new CoordinateArraySequence(Array(spatialCoord.asInstanceOf[Coordinate])), factory) {

  /**
   * create point using default GeometryFactory(srid=4326)
   *
   * @param spatialCoord spatial coordinate containing meta info
   */
  def this(spatialCoord: SpatialCoord) {
    this(spatialCoord, SpatialUtils.defaultGeomFactory)
  }

  /**
   * construct point by longitude and latitude
   *
   * @param lon longitude
   * @param lat latitude
   */
  def this(lon: Double, lat: Double) {
    this(new SpatialCoord(lon, lat))
  }

  /**
   * create point using default GeometryFactory(srid=4326)
   *
   * @param coordinate spatial coordinate without meta
   */
  def this(coordinate: Coordinate) {
    this(coordinate.getX, coordinate.getY)
  }

  /**
   * get longitude
   */
  def getLon: Double = getX

  /**
   * get latitude
   */
  def getLat: Double = getY
}
