package org.apache.spark.model.st

import java.sql.Timestamp

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.impl.CoordinateArraySequence

class STCoordSequence(var stCoords: Array[STCoord])
  extends CoordinateArraySequence(stCoords.map(_.asInstanceOf[Coordinate])) {

  /**
   * get st coord sequence
   */
  def getStCoords: Array[STCoord] = stCoords

  override def createCoordinate(): Coordinate = new STCoord(0.0, 0.0, new Timestamp(0L))

  /**
   * deep copy of this Sequence
   */
  override def copy(): STCoordSequence = {
    val cloneCoordinates = for (stCoord <- stCoords) yield {
      val duplicate = createCoordinate().asInstanceOf[STCoord]
      duplicate.setCoordinate(stCoord)
      duplicate
    }
    new STCoordSequence(cloneCoordinates)
  }
}
