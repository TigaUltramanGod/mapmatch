package org.apache.spark.mapmatch.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.model.st.spatial.{SpatialCoord, SpatialCoordSequence}

class SpatialCoordSeqSerializer extends Serializer[SpatialCoordSequence] {
  override def write(kryo: Kryo, output: Output, spatialCoordSeq: SpatialCoordSequence): Unit = {
    output.writeVarInt(spatialCoordSeq.size(), true)
    for (spatialCoord <- spatialCoordSeq.getSpatialCoords) {
      output.writeDouble(spatialCoord.getLon)
      output.writeDouble(spatialCoord.getLat)
    }
  }

  /**
    * read object which can't be null
    *
    * @param kryo  kryo
    * @param input input
    * @param clazz clazz
    **/
  override def read(kryo: Kryo, input: Input, clazz: Class[SpatialCoordSequence]): SpatialCoordSequence = {
    val size = input.readVarInt(true)
    val spatialCoords = for (_ <- 0 until size) yield {
      val lon = input.readDouble()
      val lat = input.readDouble()
      new SpatialCoord(lon, lat)
    }
    new SpatialCoordSequence(spatialCoords.toArray)
  }
}
