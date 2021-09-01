package org.apache.spark.mapmatch.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.model.st.{STCoordSequence, Trajectory}


class TrajectorySerializer extends Serializer[Trajectory] {
  /**
    * write object which can't be null
    *
    * @param kryo   kryo
    * @param output output
    * @param traj   obj
    **/
  override def write(kryo: Kryo, output: Output, traj: Trajectory): Unit = {
    output.writeString(traj.getOid)
    kryo.writeObject(output, traj.getStCoordSequence)
  }

  /**
    * read object which can't be null
    *
    * @param kryo  kryo
    * @param input input
    * @param clazz clazz
    **/
  override def read(kryo: Kryo, input: Input, clazz: Class[Trajectory]): Trajectory = {
    val oid = input.readString()
    val stCoordSeq = kryo.readObject(input, classOf[STCoordSequence])
    val traj = new Trajectory(oid, stCoordSeq)
    traj
  }
}
