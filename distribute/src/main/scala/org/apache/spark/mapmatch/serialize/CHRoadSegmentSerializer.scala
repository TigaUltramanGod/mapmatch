package org.apache.spark.mapmatch.serialize

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import contractionhierarchy.CHRoadSegment
import org.apache.spark.model.st.spatial.graph.RoadNode
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionVertex


class CHRoadSegmentSerializer extends Serializer[CHRoadSegment] {
  override def write(kryo: Kryo, output: Output, chEdge: CHRoadSegment): Unit = {
    output.writeVarInt(chEdge.getChRoadSegmentId, true)
    kryo.writeObject(output, chEdge.getSourceCHNode)
    kryo.writeObject(output, chEdge.getTargetCHNode)
    output.writeDouble(chEdge.getWeight)
    output.writeBoolean(chEdge.isUpward)
    output.writeVarInt(chEdge.getOriginRoadSegmentId, true)
    output.writeVarInt(chEdge.getReferenceCHFirstEdgeId, true)
    output.writeVarInt(chEdge.getReferenceCHSecondEdgeId, true)
    output.writeVarInt(chEdge.getOriginalEdgeCount, true)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[CHRoadSegment]): CHRoadSegment = {
    val edgeId = input.readVarInt(true)
    val sourceVertex = kryo.readObject(input, classOf[ContractionVertex[RoadNode]])
    val targetVertex = kryo.readObject(input, classOf[ContractionVertex[RoadNode]])
    val weight = input.readDouble()
    val isUpward = input.readBoolean()
    val originalRoadSegmentId = input.readVarInt(true)
    val firstEdgeId = input.readVarInt(true)
    val secondEdgeId = input.readVarInt(true)
    val count = input.readVarInt(true)
    new CHRoadSegment(edgeId, sourceVertex, targetVertex, weight, isUpward, originalRoadSegmentId, firstEdgeId, secondEdgeId, count)
  }

}
