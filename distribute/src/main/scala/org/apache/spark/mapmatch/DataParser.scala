package org.apache.spark.mapmatch

import com.alibaba.fastjson.JSON
import contractionhierarchy.{CHAccessor, CHRoadSegment}
import org.apache.spark.model.st.{STCoord, STCoordSequence, Trajectory}
import org.apache.spark.model.st.spatial.{SpatialCoord, SpatialCoordSequence}
import org.apache.spark.model.st.spatial.graph.RoadSegment
import org.apache.spark.utils.WKTUtils

import scala.collection.JavaConverters._
import java.sql.Timestamp
import java.util

object DataParser {
  def recoverCHRoad(chRoadStr: String): CHRoadSegment = CHAccessor.read(chRoadStr)

  def recoverTraj(trajString: String): Trajectory = {
    val correctStr = trajString.replaceFirst("\\[", "[\"")
      .replaceFirst(",", "\",")
    val result = JSON.parseArray(correctStr, classOf[String]).asScala

    require(result.size == 2,
      throw new IllegalArgumentException(s"require two attributes: oid and point series, but found ${result.size}"))

    val (oid, pointSeriesString) = (result.head, result.last)
    val pointSeries = JSON.parseArray(pointSeriesString, classOf[String]).asScala.map(i => {
      val element = JSON.parseArray(i, classOf[String]).asScala
      val size = element.size
      require(size == 3, throw new IllegalArgumentException(s"require three attributes: lng, lat, time, but found $size"))
      element
    })

    val stCoords = pointSeries.map(stStr => {
      new STCoord(stStr(1).toDouble, stStr.last.toDouble, Timestamp.valueOf(stStr.head))
    })
    new Trajectory(oid, new STCoordSequence(stCoords.toArray))
  }

  def recoverRoadSegment(str: String): RoadSegment = {
    val attrs = str.split("\\|")
    val oid = attrs(0).toInt
    val startId = attrs(2).toInt
    val endId = attrs(3).toInt
    val direction = attrs(4).toInt
    val level = attrs(5).toInt
    val speedLimit = attrs(6).toDouble
    val lengthInM = attrs(7).toDouble
    val spatialCoords = WKTUtils.read(attrs(1)).getCoordinates.map(new SpatialCoord(_))
    val spatialCoordSeq = new SpatialCoordSequence(spatialCoords)

    new RoadSegment(oid, startId, endId, spatialCoordSeq)
      .setDirection(direction)
      .setLevel(level)
      .setLength(lengthInM / 1000)
      .setSpeedLimit(speedLimit)
  }

  def recoverCache(lines: Array[String]): java.util.Map[(Integer, Integer), java.lang.Double] = {
    val cacheMap = new util.HashMap[(Integer, Integer), java.lang.Double](lines.length)
    lines.foreach(line => {
      val attrs = line.split(",")
      cacheMap.put((attrs(0).toInt, attrs(1).toInt), attrs(2).toDouble)
    })
    cacheMap
  }
}
