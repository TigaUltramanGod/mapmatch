package org.apache.spark.model.st

import org.apache.spark.model.st.spatial.SpatialLine
import org.apache.spark.model.st.temporal.TemporalLine

class STLine(stCoordSeq: STCoordSequence)
  extends SpatialLine(stCoordSeq) with TemporalLine[STCoord] {

  /**
    * time entities
    */
  override var temporalEntities: Array[STCoord] = stCoordSeq.getStCoords

  /**
    * get st coord sequence
    */
  def getStCoordSequence: STCoordSequence = stCoordSeq
}
