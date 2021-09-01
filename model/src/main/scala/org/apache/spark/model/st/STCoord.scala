package org.apache.spark.model.st

import java.sql.Timestamp

import org.apache.spark.model.st.spatial.SpatialCoord
import org.apache.spark.model.st.temporal.TemporalEntity
import org.locationtech.jts.geom.Coordinate

class STCoord(lon: Double, lat: Double, time: Timestamp)
  extends SpatialCoord(lon, lat) with TemporalEntity {

  /**
   * get time attribute
   */
  override def getTime: Timestamp = time
}
