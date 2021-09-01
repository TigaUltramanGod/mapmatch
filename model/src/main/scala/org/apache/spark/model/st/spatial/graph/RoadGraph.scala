package org.apache.spark.model.st.spatial.graph

import org.jgrapht.graph.{AbstractBaseGraph, DefaultGraphType}
import org.jgrapht.util.SupplierUtil

import scala.collection.JavaConversions._

/**
 * road network is an allowLoops and weighted graph
 * may be directed or undirected
 * */
class RoadGraph(directed: Boolean) extends
  AbstractBaseGraph[RoadNode, RoadSegment](
    null, SupplierUtil.createSupplier(classOf[RoadSegment]),
    new DefaultGraphType.Builder(directed, !directed)
      .weighted(true)
      .allowMultipleEdges(false).
      allowSelfLoops(true).build()) {

  /**
   * add road segment as graph edge
   *
   * @param roadSegment road segment
   * */
  def addEdge(roadSegment: RoadSegment): Boolean = {
    addVertex(roadSegment.getStartNode)
    addVertex(roadSegment.getEndNode)
    super.addEdge(roadSegment.getStartNode, roadSegment.getEndNode, roadSegment)
    setEdgeWeight(roadSegment, roadSegment.getLengthInM)
    true
  }

  /**
   * the default weight is road length
   * this method support user defined weight by offer a weight calculating function
   *
   * @param weightFunc weight calculating function
   * */
  def setWeight(weightFunc: RoadSegment => Double): Unit = {
    for (edge <- edgeSet()) {
      setEdgeWeight(edge, weightFunc(edge))
    }
  }
}


