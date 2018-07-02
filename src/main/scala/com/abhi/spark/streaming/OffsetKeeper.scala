package com.abhi.spark.streaming

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

object OffsetKeeper {

  val zkUtils = ZkUtils.apply("127.0.0.1:2181", 10000, 10000, false)
  val zkPath = "/consumers/test-consumer-group/offsets"

  // Above values should be read from application.conf

  def commitKafkaOffsets(offsets: Array[OffsetRange]): Unit = {
    val zkGroupTopicDirs = new ZKGroupTopicDirs("test-consumer-group", "test1");
    val offsetPath = zkGroupTopicDirs.consumerOffsetDir

    offsets.foreach(or => {
      zkUtils.updatePersistentPath(zkPath + "/" + or.topic + "/" + or.partition, or.untilOffset + "")
    })
  }

  def readOffsets(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)

    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition => {
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition
        try {
          val offsetStatTuple = zkUtils.readData(offsetPath)
          if (offsetStatTuple != null) {
            println(offsetStatTuple._1 + " === " + offsetStatTuple._2)
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)),
              offsetStatTuple._1.toLong)
          }
        } catch {
          case e: Exception =>
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
        }
      })
    })
    topicPartOffsetMap.toMap
  }
}