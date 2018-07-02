package com.abhi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._

class AppStreamingContext[A](sparkConf: SparkConf, kafkaParams: Map[String, Object]) extends GenericStreamingContext[A](sparkConf, kafkaParams) {
  override def batchInterval: Duration = Seconds(30)

  // Configure below setting from application.conf
  override def topics: Set[String] = Set("topic1","topic2")
  override def groupID: String = "test-consumer-group"


  override def schema: StructType = {
    val jsonNode = (new StructType)
      //Parse your JSON here
    jsonNode
  }

  override def dataFrame(inputDF: DataFrame): DataFrame = {
    inputDF.show()
    inputDF.printSchema()
    inputDF
  }
}