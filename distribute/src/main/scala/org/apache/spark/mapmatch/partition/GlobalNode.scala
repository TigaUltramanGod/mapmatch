package org.apache.spark.mapmatch.partition


trait GlobalNode extends Serializable {
  protected var partitionId: Int = -1

  def setPartitionId(id: Int): Unit = this.partitionId = id

  def getPartitionId: Int = this.partitionId
}
