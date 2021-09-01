package org.apache.spark.mapmatch.serialize

import java.sql.Timestamp

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.model.st.{STCoord, STCoordSequence}

class STCoordSeqSerializer extends Serializer[STCoordSequence] {
  override def write(kryo: Kryo, output: Output, stCoordSeq: STCoordSequence): Unit = {
    output.writeVarInt(stCoordSeq.size(), true)
    for (stCoord <- stCoordSeq.getStCoords) {
      output.writeDouble(stCoord.getLon)
      output.writeDouble(stCoord.getLat)
      kryo.writeObject(output, stCoord.getTime)
    }
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[STCoordSequence]): STCoordSequence = {
    val size = input.readVarInt(true)
    val stCoords = for (_ <- 0 until size) yield {
      val lon = input.readDouble()
      val lat = input.readDouble()
      val time = kryo.readObject(input, classOf[Timestamp])
      new STCoord(lon, lat, time)
    }
    new STCoordSequence(stCoords.toArray)
  }
}
