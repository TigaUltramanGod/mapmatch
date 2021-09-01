package org.apache.spark.model.st.temporal

import java.sql.Timestamp

trait TemporalEntity {
  /**
    * get time attribute
    **/
  def getTime: Timestamp

  /**
    * compare two time entity by time
    *
    * @param otherTimeEntity time entity
    **/
  def compareTime(otherTimeEntity: TemporalEntity): Int = {
    if (null == otherTimeEntity) return 1
    this.getTime.compareTo(otherTimeEntity.getTime)
  }
}
