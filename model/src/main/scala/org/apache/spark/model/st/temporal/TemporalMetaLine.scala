package org.apache.spark.model.st.temporal

import java.sql.Timestamp

class TemporalMetaLine(temporalMetas: Array[TemporalMeta])
  extends TemporalLine[TemporalMeta] {

  /**
    * temporal entities
    **/
  override protected var temporalEntities: Array[TemporalMeta] = temporalMetas

  /**
    * get time by index
    *
    * @param i index
    **/
  def getTime(i: Int): Timestamp = get(i).getTime
}
