package org.apache.spark.model.st.spatial.graph

import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.{Geometries, Rectangle}
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation._

class RoadNetwork(val id2SegmentMapping: Map[Int, RoadSegment]) extends Serializable {

  //todo graph 设为[roadNode, roadSegmentID]
  // map match 多点对多点寻求最短路径

  private val expandId2SegmentMapping: Map[Int, RoadSegment] = expandSegments()

  def this(roadSegments: Array[RoadSegment]) {
    //validated id and road segment mapping
    this(roadSegments
      .map(_.validate())
      .map(segment => (segment.getRoadId, segment))
      .toMap)
  }

  def this(roadSegments: Array[RoadSegment], chGraph: ContractionHierarchy[RoadNode, RoadSegment]) {
    //validated id and road segment mapping
    this(roadSegments
      .map(_.validate())
      .map(segment => (segment.getRoadId, segment))
      .toMap)
    this.chGraph = chGraph
  }

  /**
   * directed road graph
   * */
  @transient private var directedGraph: RoadGraph = _

  /**
   * directed CH road graph
   * */
  @transient private var chGraph: ContractionHierarchy[RoadNode, RoadSegment] = _

  /**
   * undirected road graph
   * */
  @transient private var undirectedGraph: RoadGraph = _

  /**
   * road rtree,once it builds, only support query, not support delete and insert
   * */
  @transient private var roadRtree: RTree[RoadSegment, Rectangle] = _

  /**
   * get number of road segment
   * */
  def getRoadNum: Int = expandId2SegmentMapping.size

  /**
   * get road segments
   * */
  def getRoadSegments: Array[RoadSegment] = expandId2SegmentMapping.values.toArray

  /**
   * get road segment by road id
   *
   * @param roadId road id
   * @return road segment
   * */
  def getRoadSegment(roadId: Int): RoadSegment = {
    val option = expandId2SegmentMapping.get(roadId)
    if (option.isDefined) option.get
    else throw new IllegalArgumentException(s"there is no road segment for id:{$roadId}")
  }

  /**
   * get road graph, if it's null,build it
   * */
  def getRoadGraph(directed: Boolean = true): RoadGraph = this.synchronized {
    if (directed) {
      if (directedGraph == null) {
        directedGraph = new RoadGraph(true)
        expandId2SegmentMapping.values.foreach(directedGraph.addEdge)
      }
      return directedGraph
    }
    if (undirectedGraph == null) {
      undirectedGraph = new RoadGraph(false)
      id2SegmentMapping.values.foreach(undirectedGraph.addEdge)
    }
    undirectedGraph
  }

  /**
   * get ch road graph
   * */
  def getCHGraph: ContractionHierarchy[RoadNode, RoadSegment] = chGraph

  /**
   * expand dual way for this road network
   * */
  private def expandSegments(): Map[Int, RoadSegment] = {
    var expandMapping = id2SegmentMapping
    for ((_, rawSegment) <- id2SegmentMapping) {
      rawSegment.testForward() match {
        case Some(forwardSegment) =>
          expandMapping += (forwardSegment.getRoadId -> forwardSegment)
        case _ =>
      }
    }
    expandMapping
  }

  /**
   * get road segment Rtree index, if it's null, build it
   * */
  def getRoadRtree: RTree[RoadSegment, Rectangle] = this.synchronized {
    if (null == roadRtree) {
      roadRtree = RTree.create[RoadSegment, Rectangle]
      expandId2SegmentMapping.values.foreach(roadSegment => {
        roadRtree = roadRtree.add(
          roadSegment, Geometries.rectangleGeographic(
            roadSegment.getMbr.getEnvelopeInternal.getMinX, roadSegment.getMbr.getEnvelopeInternal.getMinY,
            roadSegment.getMbr.getEnvelopeInternal.getMaxX, roadSegment.getMbr.getEnvelopeInternal.getMaxY))
      })
      roadRtree
    }
    else roadRtree
  }

  def setCHGraph(graph: ContractionHierarchy[RoadNode, RoadSegment]): Unit = {
    chGraph = graph
  }

  override def toString: String = {
    s"RoadNetwork{ segmentNum: ${id2SegmentMapping.size}, contract: ${chGraph != null} }"
  }
}
