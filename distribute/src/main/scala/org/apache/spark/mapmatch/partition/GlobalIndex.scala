package org.apache.spark.mapmatch.partition

import org.locationtech.jts.geom.{Envelope, Geometry, Point}

import scala.collection.mutable.ArrayBuffer

trait GlobalIndex extends Serializable {

  def findNearestId(queryCentre: Point): Int

  def findIntersectIds(queryEnv: Envelope, idCollector: ArrayBuffer[Int]): Unit

  def assignPartitionId(baseId: Int): Int

  def getLeafEnv(index: Int): Envelope

  def getPartitionId(queryGeom: Geometry): Int = {
    val queryCentre = queryGeom.getFactory.createPoint(queryGeom.getEnvelopeInternal.centre())
    findNearestId(queryCentre)
  }

  def getPartitionIds(queryGeom: Geometry, distance: Double = 0.0): Array[Int] = {
    val idCollector = new ArrayBuffer[Int]()
    val geomEnv = queryGeom.getEnvelopeInternal
    if (distance > 0.0) geomEnv.expandBy(distance)
    findIntersectIds(geomEnv, idCollector)
    idCollector.toArray
  }
}
