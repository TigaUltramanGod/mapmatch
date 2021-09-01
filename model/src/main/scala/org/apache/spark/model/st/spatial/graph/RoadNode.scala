package org.apache.spark.model.st.spatial.graph

import org.apache.spark.model.st.spatial.{SpatialCoord, SpatialPoint}
import org.locationtech.jts.geom.Geometry

class RoadNode(val nodeId: Int, spatialCoord: SpatialCoord) extends SpatialPoint(spatialCoord) {

  /**
   * hashcode equals nodeId
   * */
  override def hashCode: Int = nodeId

  /**
   * compare road node by nodeId and spatial coordinate
   * */
  override def equals(o: Any): Boolean = {
    o.isInstanceOf[RoadNode] &&
      o.asInstanceOf[RoadNode].nodeId == this.nodeId
  }

  /**
   * compare road node by nodeId and spatial coordinate
   * */
  override def equals(o: Geometry): Boolean = {
    o.isInstanceOf[RoadNode] &&
      o.asInstanceOf[RoadNode].nodeId == this.nodeId
  }

  override def toString: String = {
    s"$nodeId,${super.getLon},${super.getLat}"
  }
}
