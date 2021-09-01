package org.apache.spark.model.st

import java.sql.Timestamp


import org.apache.spark.model.st.spatial.SpatialLine
import org.apache.spark.model.st.spatial.graph.{RoadNetwork, RoadNode}
import org.apache.spark.model.st.temporal.{TemporalEntity, TemporalLine}
import org.locationtech.jts.geom.Envelope

/**
 * Route of Trajectory for constructing Trajectory MapMatch result
 *
 * @param endTime   endTime of route of trajectory
 * @param isSpatial whether subRoutes contains spatial attribute
 */
class RouteOfTrajectory(val oid: String,
                        val subRoutes: Array[SubRoute],
                        val endTime: Timestamp,
                        var isSpatial: Boolean) extends TemporalLine[SubRoute] {
  /**
   * mbr of route of trajectory
   */
  private var mbr: Envelope = _

  /**
   * start node of route of trajectory
   */
  private var startNode: RoadNode = _

  /**
   * end node of route of trajectory
   */
  private var endNode: RoadNode = _

  /**
   * temporal entities
   */
  override protected var temporalEntities: Array[SubRoute] = subRoutes

  /**
   * get end time of route of trajectory
   */
  override def getEndTime: Timestamp = endTime

  /**
   * get mbr of route of trajectory
   */
  def getMbr: Envelope = {
    if (mbr == null) {
      mbr = new Envelope()
      if (isSpatial) {
        subRoutes.foreach(subRoute =>
          mbr.expandToInclude(subRoute.spatialLine.get.getEnvelopeInternal)
        )
      } else {
        throw new RuntimeException("route of trajectory doesn't contain spatial attribute")
      }
    }
    mbr
  }

  /**
   * get start node of route of trajectory
   */
  def getStartNode: RoadNode = {
    if (startNode == null) {
      if (isSpatial) {
        startNode = new RoadNode(
          subRoutes.head.roadSegmentId,
          subRoutes.head.spatialLine.get.getFirstCoordinate
        )
      } else {
        throw new RuntimeException("route of trajectory doesn't contain spatial attribute")
      }
    }
    startNode
  }

  /**
   * get end of route of trajectory
   */
  def getEndNode: RoadNode = {
    if (endNode == null) {
      if (isSpatial) {
        endNode = new RoadNode(
          subRoutes.last.roadSegmentId,
          subRoutes.last.spatialLine.get.getLastCoordinate
        )
      } else {
        throw new RuntimeException("route of trajectory doesn't contain spatial attribute")
      }
    }
    endNode
  }

  /**
   * set mbr
   *
   * @param mbr mbr
   */
  def setMbr(mbr: Envelope): Unit = {
    this.mbr = mbr
  }

  /**
   * set start node of route of trajectory
   *
   * @param startNode start node
   */
  def setStartNode(startNode: RoadNode): Unit = {
    this.startNode = startNode
  }

  /**
   * set end node of route of trajectory
   *
   * @param endNode end node
   */
  def setEndNode(endNode: RoadNode): Unit = {
    this.endNode = endNode
  }

  /**
   * get the time range of sub route i
   *
   * @param i index of sub route
   * @return start and end time of the i-th sub route
   */
  def getTimeRange(i: Int): (Timestamp, Timestamp) = {
    if (i >= subRoutes.length)
      throw new IndexOutOfBoundsException(s"index out of bound:$i > ${subRoutes.length - 1}")
    val leaveTime = if (i == subRoutes.length - 1) endTime else subRoutes(i + 1).getTime
    (subRoutes(i).getTime, leaveTime)
  }

  /**
   * recover spatial coordinate sequence for sub routes
   *
   * @param roadNetwork road network used for generate those sub routes
   */
  def recover(roadNetwork: RoadNetwork): Unit = {
    subRoutes.foreach(subRoute => {
      if (subRoute.spatialLine.isEmpty) {
        subRoute.setCoordSeq(roadNetwork.getRoadSegment(subRoute.roadSegmentId))
      }
    })
    isSpatial = true
  }

  override def toString: String = {
    String.format("%s,%s", oid, subRoutes.mkString("Array(", ", ", ")"))
  }
}


class SubRoute(val roadSegmentId: Int, enterTime: Timestamp,
               var spatialLine: Option[SpatialLine] = None) extends TemporalEntity {

  /**
   * get time attribute
   */
  override def getTime: Timestamp = enterTime

  /**
   * set the spatial coordinate sequence for sub route
   *
   * @param spatialLine spatial line
   */
  def setCoordSeq(spatialLine: SpatialLine): Unit = {
    this.spatialLine = Some(spatialLine)
  }

  override def toString: String =
    String.format("%s,\"%s\"", roadSegmentId.toString, enterTime)
}

