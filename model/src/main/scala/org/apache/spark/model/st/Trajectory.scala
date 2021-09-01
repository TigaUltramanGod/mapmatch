package org.apache.spark.model.st

import java.util

import org.locationtech.jts.geom.LineString

import scala.collection.JavaConverters._

class Trajectory(oid: String, stCoordSeq: STCoordSequence) extends STLine(stCoordSeq) {

  def this(oid: String, stCoords: Array[STCoord]) {
    this(oid, new STCoordSequence(
      if (stCoords.length == 1) {
        val newCoords = stCoords.toBuffer
        newCoords ++= stCoords
        newCoords.toArray
      } else stCoords))
  }

  /**
    * for convenient of java calling
    *
    * @param oid      object id
    * @param stCoords st coordinate
    */
  def this(oid: String, stCoords: util.List[STPoint]) {
    this(oid, stCoords.asScala.map(_.getSTCoord).toArray)
  }

  def getOid: String = this.oid

  def getLineString: LineString = getFactory.createLineString(points)

  def getSTPointList: java.util.List[STPoint] = {
    val list = new util.ArrayList[STPoint]()
    stCoordSeq.getStCoords.map(stCoord => list.add(new STPoint(stCoord)))
    list
  }

  /**
    * show the st_series String to front end
    *
    * @return : java.lang.String
    */
  override def toString: String = {
    val points = stCoordSeq.getStCoords.map(
      point => {
        val time = point.getTime
        val lng = point.getX
        val lat = point.getY
        Seq(time, lng, lat).map {
          case i@(_: Number) => i.toString
          case i => s""""${i.toString}""""
        }.mkString("[", ",", "]")
      }).mkString("[", ",", "]")
    val traj = Seq(getOid, points)
    traj.mkString("[", ",", "]")
  }
}
