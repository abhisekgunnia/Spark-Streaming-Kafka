package com.abhi.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf


/*
  * This class serves as a boiler plate and can configured through custom application.conf
  * */

object AppMain {
  def getKafkaParams: Map[String, Object] = {
    val kafkaParams = Map(
      "bootstrap.servers" -> "",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }
  val sparkConf = new SparkConf().setAppName("AppMain").setMaster("local[4]")

  val myApp = new AppStreamingContext(sparkConf,getKafkaParams)

  myApp.process("Location on HDFS")

  myApp.streamingContext.start()
  myApp.streamingContext.awaitTermination()

  // Use graceful shutdown for testing
  //myApp.streamingContext.stop(true,true)

}