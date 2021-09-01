package org.apache.spark.mapmatch.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.model.st.spatial.SpatialCoordSequence
import org.apache.spark.model.st.spatial.graph.RoadSegment

class RoadSegmentSerializer extends Serializer[RoadSegment] {
  override def write(kryo: Kryo, output: Output, roadSegment: RoadSegment): Unit = {
    kryo.writeObject(output, roadSegment.getCoordSequence)
    output.writeVarInt(roadSegment.getRoadId, true)
    output.writeVarInt(roadSegment.getStartId, true)
    output.writeVarInt(roadSegment.getEndId, true)
    output.writeVarInt(roadSegment.getDirection, true)
    output.writeVarInt(roadSegment.getLevel, true)
    output.writeDouble(roadSegment.getSpeedLimit)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[RoadSegment]): RoadSegment = {
    val spatialCoordSeq = kryo.readObject(input, classOf[SpatialCoordSequence])
    val roadId = input.readVarInt(true)
    val startId = input.readVarInt(true)
    val endId = input.readVarInt(true)
    new RoadSegment(roadId, startId, endId, spatialCoordSeq)
      .setDirection(input.readVarInt(true))
      .setLevel(input.readVarInt(true))
      .setSpeedLimit(input.readDouble())
  }
}