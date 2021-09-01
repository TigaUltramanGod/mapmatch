package org.apache.spark.model.st.spatial

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.impl.CoordinateArraySequence

class SpatialCoordSequence(spatialCoords: Array[SpatialCoord])
  extends CoordinateArraySequence(spatialCoords.map(_.asInstanceOf[Coordinate])) {

  /**
   * get spatial coordinates with meta
   */
  def getSpatialCoords: Array[SpatialCoord] = spatialCoords

  /**
   * create an empty coordinate used in this sequence(SpatialCoord)
   */
  override def createCoordinate(): Coordinate = new SpatialCoord(0.0, 0.0)

  /**
   * deep copy of this Sequence
   */
  override def copy(): SpatialCoordSequence = {
    val cloneCoordinates = for (spatialCoord <- spatialCoords) yield {
      val duplicate = createCoordinate().asInstanceOf[SpatialCoord]
      duplicate.setCoordinate(spatialCoord)
      duplicate
    }
    new SpatialCoordSequence(cloneCoordinates)
  }
}