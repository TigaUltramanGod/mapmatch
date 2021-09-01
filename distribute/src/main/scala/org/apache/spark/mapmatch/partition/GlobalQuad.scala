package org.apache.spark.mapmatch.partition

import java.util.{Comparator, PriorityQueue}

import org.locationtech.jts.geom.{Envelope, Point}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._


class GlobalQuad(spatialBound: Envelope) extends GlobalIndex {
  private val root = new QuadNode(spatialBound)
  private var leafNodes: Array[QuadNode] = _

  def build(samples: Array[Envelope],  leafNum: Int): Unit = {
    val comparator = new Comparator[(QuadNode, Array[Envelope])] {
      override def compare(o1: (QuadNode, Array[Envelope]), o2: (QuadNode, Array[Envelope])): Int = {
        0 - Integer.compare(o1._2.length, o2._2.length)
      }
    }
    val queue = new PriorityQueue[(QuadNode, Array[Envelope])](comparator)
    queue.add((root, samples))
    while (queue.size() < leafNum) {
      val (maxNode, maxSamples) = queue.poll()
      maxNode.split(maxSamples, queue)
    }
    this.leafNodes = queue.asScala.map(_._1).toArray
  }

  override def findNearestId(queryCentre: Point): Int = {
    root.findNearestId(queryCentre)
  }

  override def findIntersectIds(queryEnv: Envelope,
                                idCollector: ArrayBuffer[Int]): Unit = {
    root.findIntersectIds(queryEnv, idCollector)
  }

  override def assignPartitionId(baseId: Int): Int = {
    for (i <- leafNodes.indices) {
      leafNodes(i).setPartitionId(baseId + i)
    }
    baseId + leafNodes.length
  }

  override def getLeafEnv(index: Int): Envelope = leafNodes(index).env
}
