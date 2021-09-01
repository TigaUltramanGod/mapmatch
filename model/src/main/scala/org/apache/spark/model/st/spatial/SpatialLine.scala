package org.apache.spark.model.st.spatial

import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import point.GeoFunction

/**
 * srid of default GeometryFactory is 4326
 */
class SpatialLine(coordinateArraySeq: CoordinateArraySequence, factory: GeometryFactory = new GeometryFactory())
  extends LineString(coordinateArraySeq, factory) {

  require(null != coordinateArraySeq, "spatial coord sequence can't be null")

  /**
   * spatial length(KiloMeter)
   */
  protected var lengthInKM: Double = -1.0

  /**
   * get n-th spatial coord
   *
   * @param n index
   */
  override def getCoordinateN(n: Int): SpatialCoord = {
    super.getCoordinateN(n).asInstanceOf[SpatialCoord]
  }

  /**
   * get start coord
   */
  def getFirstCoordinate: SpatialCoord = getCoordinateN(0)

  /**
   * get last spatial coord
   */
  def getLastCoordinate: SpatialCoord = getCoordinateN(getNumPoints - 1)

  /**
   * set length if is pre calculated
   *
   * @param lengthInKM length of spatial line
   */
  def setLengthInKM(lengthInKM: Double): Unit = {
    this.lengthInKM = lengthInKM
  }

  /**
   * get spatial length(Meter)
   * */
  def getLengthInM: Double = this.synchronized {
    val subLengthArray = for (index <- 0 until getNumPoints - 1) yield {
      GeoFunction.getDistanceInM(getPointN(index), getPointN(index + 1))
    }
    subLengthArray.sum
  }

  /**
   * construct bound envelope to polygon
   */
  def getMbr: Polygon = {
    val env = getEnvelopeInternal
    createPolygon(env)
  }

  /**
   * convert envelope to polygon
   *
   * @param env envelope
   */
  def createPolygon(env: Envelope): Polygon = {
    factory.createPolygon(
      factory.createLinearRing(Array[Coordinate](
        new Coordinate(env.getMinX, env.getMinY),
        new Coordinate(env.getMinX, env.getMaxY),
        new Coordinate(env.getMaxX, env.getMaxY),
        new Coordinate(env.getMaxX, env.getMinY),
        new Coordinate(env.getMinX, env.getMinY)
      )), null)
  }

  override def getLength: Double = getLengthInM

  /**
   * filter spatial line by mbr
   *
   * @param mbr minimum bound rectangle represented by Polygon
   */
  def filterByMbr(mbr: Polygon): Seq[Array[SpatialCoord]] = {
    val intersection = this.intersection(mbr)
    intersection match {
      case line: LineString =>
        //intersection only contains one sub line
        Seq(line.getCoordinates.map(_.asInstanceOf[SpatialCoord]))
      case multiLine: MultiLineString =>
        //intersection contains more than one sub line
        for (num <- 0 until multiLine.getNumGeometries) yield
          multiLine.getGeometryN(num).getCoordinates.map(_.asInstanceOf[SpatialCoord])
      case _ =>
        throw new RuntimeException(s"spatial line mbr filter result ${intersection.getClass.getName} is not held")
    }
  }
}
