package org.apache.spark.mapmatch

import mapmatch.shortestpath.ShortestPathAlgoTypeEnum
import mapmatch.tihmm.{TiHmmMapMatcher, TiHmmMultipleMatcher}
import org.apache.spark.mapmatch.partition.GlobalQuad
import org.apache.spark.model.st.spatial.graph.{RoadNetwork, RoadSegment}
import org.apache.spark.model.st.{RouteOfTrajectory, Trajectory}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation
import org.locationtech.jts.geom.Envelope
import point.GeoFunction
import DataParser._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object PartitionSpaceApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("PartitionSpaceApp")
    val context = new SparkContext(sparkConf)

    val startTime = System.currentTimeMillis()

    val trajPath = args(0)
    val rnPath = args(1)
    val outputPath = args(2)

    val roadRdd = context.textFile(rnPath).map(recoverRoadSegment)
    val trajRdd = context.textFile(trajPath).map(recoverTraj)
    val trajSmaples = trajRdd.sample(withReplacement = false, 0.1).map(_.getEnvelopeInternal).collect()

    val globalEnv = new Envelope(108.843960232205, 109.07412624783, 34.1552039930556, 34.3295857747396)
    val globalIndex = new GlobalQuad(globalEnv)
    globalIndex.build(trajSmaples, 20)
    val partitionNum = globalIndex.assignPartitionId(0)
    val bcGlobalIndex = context.broadcast(globalIndex)

    //spatial partition
    val partitioner = new Partitioner {
      override def numPartitions: Int = partitionNum

      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }
    val partitionRoadRdd = roadRdd.mapPartitions(roadIter => {
      val index = bcGlobalIndex.value
      val expandDist = GeoFunction.getDegreeFromM(500)
      roadIter.flatMap(road => {
        index.getPartitionIds(road, expandDist).map((_, road))
      })
    }).partitionBy(partitioner).map(_._2)
    val partitionTrajRdd = trajRdd.mapPartitions(trajIter => {
      val index = bcGlobalIndex.value
      trajIter.flatMap(traj => {
        val ids = index.getPartitionIds(traj)
        if (ids.length == 1) {
          ids.map((_, traj))
        } else {
          ids.flatMap(id => {
            val env = index.getLeafEnv(id)
            val indexBounds = new ArrayBuffer[(Int, Int)]()
            var startIndex = -1
            var endIndex = -1
            var flag = true
            val stCoords = traj.getStCoordSequence.stCoords
            for (index <- 0 until traj.getNumPoints) {
              if (env.contains(stCoords(index))) {
                if (flag) {
                  startIndex = index
                  flag = !flag
                }
              } else {
                if (!flag) {
                  endIndex = index
                  indexBounds += ((startIndex, endIndex))
                  startIndex = -1
                  endIndex = -1
                  flag = !flag
                }
              }
            }
            if (startIndex != -1 && startIndex < endIndex - 1) {
              endIndex = stCoords.length
              indexBounds += ((startIndex, endIndex))
            }

            indexBounds.map(bound => {
              val segmentCoords = stCoords.slice(bound._1, bound._2)
              (id, new Trajectory(traj.getOid, segmentCoords))
            })
          })
        }
      })
    }).partitionBy(partitioner).map(_._2)

    val count = partitionTrajRdd.zipPartitions(partitionRoadRdd)((trajIter, roadIter) => {
      val matcher = getMapMatcher(roadIter.toArray)
      trajIter.flatMap(traj => {
        val routes = matcher.matchTrajToRoute(traj)
        if (routes == null) Array.empty[RouteOfTrajectory] else routes.asScala
      })
    }).count()

    val consumingSec = (System.currentTimeMillis() - startTime) / 1E3
    println(consumingSec)
    context.parallelize(Seq(s"count:$count;time:${consumingSec}s"))
      .repartition(1).saveAsTextFile(outputPath)
  }

  def getMapMatcher(roadSegments: Array[RoadSegment]): TiHmmMapMatcher = {
    val roadNetwork = new RoadNetwork(roadSegments.map(rs => (rs.getRoadId, rs)).toMap)
    val preComputation = new ContractionHierarchyPrecomputation(roadNetwork.getRoadGraph(true))
    val ch = preComputation.computeContractionHierarchy()
    roadNetwork.setCHGraph(ch)
    new TiHmmMultipleMatcher(roadNetwork, ShortestPathAlgoTypeEnum.CH)
  }

}
