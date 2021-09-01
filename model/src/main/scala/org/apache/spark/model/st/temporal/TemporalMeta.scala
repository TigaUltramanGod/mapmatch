package org.apache.spark.model.st.temporal

import java.sql.Timestamp

class TemporalMeta(time: Timestamp) extends TemporalEntity {
  /**
    * get time attribute
    **/
  override def getTime: Timestamp = time
}
