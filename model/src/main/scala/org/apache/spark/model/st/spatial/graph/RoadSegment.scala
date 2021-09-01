package org.apache.spark.model.st.spatial.graph

import java.util

import org.apache.spark.model.st.spatial.{SpatialCoordSequence, SpatialLine, SpatialPoint}
import org.locationtech.jts.geom.LineString
import scala.collection.JavaConverters._

class RoadSegment(roadId: Int, startId: Int, endId: Int, spatialCoordSeq: SpatialCoordSequence)
  extends SpatialLine(spatialCoordSeq) {

  /**
   * start node of road segment
   * */
  private val startNode = new RoadNode(startId, getCoordinateN(0))

  /**
   * end node of road segment
   * */
  private val endNode = new RoadNode(endId, getCoordinateN(getNumPoints - 1))

  /**
   * road direction
   * 1: dual-way
   * 2: forward way
   * 3: backward way
   */
  private var direction: Int = 0

  /**
   * speed limit(KM/H)
   */
  private var speedLimit: Double = 0.0

  /**
   * level
   * 0: high speed
   * 1: 高架
   * the higher level the lower speed limit
   */
  private var level = 0

  /**
   * set direction
   *
   * @param direction road direction
   * */
  def setDirection(direction: Int): RoadSegment = {
    this.direction = direction
    this
  }

  /**
   * set speed limit
   *
   * @param speedLimit speed limit of road
   * */
  def setSpeedLimit(speedLimit: Double): RoadSegment = {
    this.speedLimit = speedLimit
    this
  }

  /**
   * set road level
   *
   * @param level road level
   * */
  def setLevel(level: Int): RoadSegment = {
    this.level = level
    this
  }

  /**
   * external road segment length replace interval length calculated by spatialCoords
   *
   * @param lengthInKM external length of road segment
   * */
  def setLength(lengthInKM: Double): RoadSegment = {
    super.setLengthInKM(lengthInKM)
    this
  }

  /**
   * get coordinate sequence
   * */
  def getCoordSequence: SpatialCoordSequence = spatialCoordSeq

  /**
   * get line string from road segment
   * */
  def getLineString: LineString = getFactory.createLineString(points)

  /**
   * get road segment id
   * */
  def getRoadId: Int = roadId

  /**
   * get start id
   * */
  def getStartId: Int = startId

  /**
   * get end id
   * */
  def getEndId: Int = endId

  /**
   * get start node of road segment
   * */
  def getStartNode: RoadNode = this.startNode

  /**
   * get end node of road segment
   * */
  def getEndNode: RoadNode = this.endNode

  /**
   * get road direction
   * */
  def getDirection: Int = this.direction

  /**
   * get speed limit
   * */
  def getSpeedLimit: Double = this.speedLimit

  /**
   * get road level
   * */
  def getLevel: Int = this.level

  /**
   * 获取在roadSegment上所有的geodeticPoint(等GeodeticPoint被完全重构后删去)
   * */
  def getCoords: util.List[SpatialPoint] = getCoordinates.map(coordinate => new SpatialPoint(coordinate)).toList.asJava

  override def hashCode: Int = roadId

  /**
   * road graph checkout if road segment already exists when calling addEdge
   * by using this method
   *
   * @param obj other road segment
   * */
  override def equals(obj: Any): Boolean = {
    obj match {
      case segment: RoadSegment =>
        this.roadId == segment.getRoadId
      case _ => false
    }
  }

  /**
   * if this road segment is a dual-way, return it's reversed road segment
   * else return None
   * */
  def testForward(): Option[RoadSegment] = {
    if (direction == RoadSegment.DUAL_DIRECT) {
      val reverseRoadId = if (roadId == 0) Int.MinValue else -roadId
      Some(safeReverse(reverseRoadId))
    } else None
  }

  /**
   * if this road segment is a backward way, reverse it and return
   * else return this road segment itself
   * */
  def validate(): RoadSegment = {
    if (direction == RoadSegment.BACKWARD_DIRECT) {
      safeReverse(roadId)
    } else this
  }

  /**
   * copy SpatialCoordSequence and reverse it
   * this method doesn't destroy original RoadSegment
   * */
  private def safeReverse(reverseRoadId: Int): RoadSegment = {
    val reverseSeq = new SpatialCoordSequence(spatialCoordSeq.getSpatialCoords.reverse)
    new RoadSegment(reverseRoadId, endId, startId, reverseSeq)
      .setDirection(direction)
      .setSpeedLimit(speedLimit)
      .setLevel(level)
      .setLength(lengthInKM)
  }
}

object RoadSegment {
  val DUAL_DIRECT = 1
  val FORWARD_DIRECT = 2
  val BACKWARD_DIRECT = 3
}
