package org.apache.spark.mapmatch

import contractionhierarchy.CHRoadSegment
import mapmatch.shortestpath.ShortestPathAlgoTypeEnum
import mapmatch.tihmm.{TiHmmCacheMatcher, TiHmmMapMatcher, TiHmmMultipleMatcher}
import org.apache.spark.mapmatch.DataParser._
import org.apache.spark.mapmatch.serialize.SerializerRegistrator
import org.apache.spark.model.st.spatial.graph.RoadSegment
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object MapMatchApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[SerializerRegistrator].getName)
      //.setMaster("local[*]")
      .setAppName("MapMatchApp")
    val sparkContext = new SparkContext(sparkConf)

    val startTime = System.currentTimeMillis()

    val trajPath = args(0)
    val rnPath = args(1)
    val cachePath = args(2)
    val chPath = args(3)
    val statPath = args(4)
    val shortAlgorithm = args(5).toInt

    val trajRdd = sparkContext.textFile(trajPath, 1024).map(recoverTraj)
    val roadSegments = sparkContext.textFile(rnPath).map(recoverRoadSegment).collect()
    val cacheMap = if (shortAlgorithm != 5) new util.HashMap[(Integer, Integer), java.lang.Double]()
    else recoverCache(sparkContext.textFile(cachePath).collect())
    val chRoadSegments = if (shortAlgorithm < 6) Array.empty[CHRoadSegment]
    else sparkContext.textFile(chPath).map(recoverCHRoad).collect()

    val bcRoadSegments = sparkContext.broadcast[Array[RoadSegment]](roadSegments)
    val bcCacheMap = sparkContext.broadcast[util.Map[(Integer, Integer), java.lang.Double]](cacheMap)
    val bcCHRoadSegments = sparkContext.broadcast[Array[CHRoadSegment]](chRoadSegments)

    val mapMatchRdd = trajRdd.mapPartitions(trajIter => {
      val mapMatcher = getMapMatcher(shortAlgorithm, bcRoadSegments.value, bcCacheMap.value, bcCHRoadSegments.value)
      trajIter.flatMap(traj => Option(mapMatcher.matchTrajToRoute(traj)))
    })

    val count = mapMatchRdd.count()
    val consumingSec = (System.currentTimeMillis() - startTime) / 1E3
    sparkContext.parallelize(Seq(s"count:$count;time:${consumingSec}s"))
      .repartition(1).saveAsTextFile(statPath)
  }

  def getMapMatcher(shortAlgorithm: Int, roadSegemnts: Array[RoadSegment],
                    cacheLines: util.Map[(Integer, Integer), java.lang.Double],
                    chRoadSegments: Array[CHRoadSegment]): TiHmmMapMatcher = {

    shortAlgorithm match {
      case 1 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, Array.empty)
        new TiHmmMapMatcher(rn, ShortestPathAlgoTypeEnum.ASTAR)
      case 2 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, Array.empty)
        new TiHmmMapMatcher(rn, ShortestPathAlgoTypeEnum.DIJKSTRA)
      case 3 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, Array.empty)
        new TiHmmMapMatcher(rn, ShortestPathAlgoTypeEnum.BI_DIJKSTRA)
      case 4 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, Array.empty)
        new TiHmmMultipleMatcher(rn, ShortestPathAlgoTypeEnum.BI_DIJKSTRA)
      case 5 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, Array.empty)
        new TiHmmCacheMatcher(rn, cacheLines)
      case 6 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, chRoadSegments)
        new TiHmmMapMatcher(rn, ShortestPathAlgoTypeEnum.CH)
      case 7 =>
        val rn = RoadNetworkContainer.getInstance("rn", roadSegemnts, chRoadSegments)
        new TiHmmMultipleMatcher(rn, ShortestPathAlgoTypeEnum.CH)
      case _ => throw new IllegalArgumentException("unrecognized algorithm type")
    }
  }
}
