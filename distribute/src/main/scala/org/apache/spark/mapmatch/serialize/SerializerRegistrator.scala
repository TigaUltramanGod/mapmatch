package org.apache.spark.mapmatch.serialize

import com.esotericsoftware.kryo.Kryo
import contractionhierarchy.CHRoadSegment
import org.apache.spark.model.st.spatial.SpatialCoordSequence
import org.apache.spark.model.st.spatial.graph.RoadSegment
import org.apache.spark.model.st.{STCoordSequence, Trajectory}
import org.apache.spark.serializer.KryoRegistrator

class SerializerRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[SpatialCoordSequence], new SpatialCoordSeqSerializer)
    kryo.register(classOf[STCoordSequence], new STCoordSeqSerializer)
    kryo.register(classOf[RoadSegment], new RoadSegmentSerializer)
    kryo.register(classOf[CHRoadSegment], new CHRoadSegmentSerializer)
    kryo.register(classOf[Trajectory], new TrajectorySerializer)
  }
}
