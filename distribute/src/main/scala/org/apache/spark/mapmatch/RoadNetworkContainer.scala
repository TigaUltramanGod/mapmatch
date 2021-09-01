package org.apache.spark.mapmatch

import contractionhierarchy.{CHRoadSegment, CHTransformer}
import org.apache.spark.model.st.spatial.graph.{RoadNetwork, RoadSegment}

import scala.collection.JavaConverters._

object RoadNetworkContainer {
  private val roadNetworkMap = scala.collection.mutable.Map.empty[String, RoadNetwork]

  /**
   * create a road network or get from the cache,if any
   *
   * @param name         road network name
   * @param roadSegments road segments for building road network
   * */
  def getInstance(name: String, roadSegments: Array[RoadSegment], chRoadSegments: Array[CHRoadSegment]): RoadNetwork = {
    val roadNetwork = roadNetworkMap.get(name)
    if (roadNetwork.isEmpty) {
      this.synchronized {
        roadNetworkMap.getOrElseUpdate(name, createRoadNetwork(roadSegments, chRoadSegments))
      }
    } else roadNetwork.get
  }


  /**
   * create road network using road segments
   *
   * @param roadSegments road segments
   * @return road network
   * */
  private def createRoadNetwork(roadSegments: Array[RoadSegment], chRoadSegments: Array[CHRoadSegment]): RoadNetwork = {
    val roadNetwork = new RoadNetwork(roadSegments.map(rs => (rs.getRoadId, rs)).toMap)
    if (chRoadSegments.nonEmpty) {
      val ch = CHTransformer.edges2Graph(chRoadSegments.toBuffer.asJava, roadNetwork)
      roadNetwork.setCHGraph(ch)
    }
    roadNetwork
  }
}
