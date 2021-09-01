package org.apache.spark.model.st.temporal

/**
  * base class for ts model
  **/
class TimeSeries[T <: TemporalEntity](tEntities: Array[T]) extends TemporalLine[T] {
  /**
    * time entities
    **/
  override protected var temporalEntities: Array[T] = tEntities

  //todo:ts model methods
}
