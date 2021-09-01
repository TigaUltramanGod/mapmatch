package org.apache.spark.model.st

import java.sql.Timestamp

import org.apache.spark.model.st.spatial.SpatialPoint
import org.apache.spark.model.st.temporal.TemporalEntity
import org.locationtech.jts.geom.Coordinate

class STPoint(stCoord: STCoord)
  extends SpatialPoint(stCoord) with TemporalEntity {

  /**
   * construct STPoint with its properties
   *
   * @param coordinate Coordinate without meta
   * @param time       time
   */
  def this(coordinate: Coordinate, time: Timestamp) {
    this(new STCoord(coordinate.getX, coordinate.getY, time))
  }


  /**
   * construct STPoint with its properties
   *
   * @param lon  longitude
   * @param lat  latitude
   * @param time time
   */
  def this(lon: Double, lat: Double, time: Timestamp) {
    this(new STCoord(lon, lat, time))
  }

  /**
   * get time attribute
   */
  override def getTime: Timestamp = stCoord.getTime

  /**
   * get spatial coordinate
   */
  def getSTCoord: STCoord = stCoord
}
