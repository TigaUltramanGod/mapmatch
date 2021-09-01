package org.apache.spark.utils

import org.locationtech.jts.geom.{Envelope, GeometryFactory, Point}

object GeomUtils {
  val defaultFactory = new GeometryFactory()

  def getCentre(env: Envelope): Point = {
    defaultFactory.createPoint(env.centre())
  }
}
