package com.abhi.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


abstract class GenericStreamingContext[A](sparkConf: SparkConf, kafkaParams: Map[String, Object]) {

  def batchInterval: Duration
  def topics: Set[String]
  def groupID : String
  def schema: StructType
  def dataFrame(inDF: DataFrame): DataFrame = inDF

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val streamingContext = new StreamingContext(sparkSession.sparkContext, batchInterval)
  val sqlContext: SQLContext = sparkSession.sqlContext

  val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent,Subscribe[String, String](topics, kafkaParams,OffsetKeeper.readOffsets(topics.toSeq,groupID)))

  def process(path: String) {
    var offsetRanges = Array[OffsetRange]()

    // Using transform on streams to apply multiple data pipelines on the streams
    // With this pattern spark batch code can be reused in steaming applications
    dstream.transform { rdd =>
      // Reading offsets from stream
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(or => {
        println(or.partition+" : "+or.topic+" : "+or.fromOffset+" : "+or.untilOffset)
      })
      rdd
    }.transform{rdd=>
        // Use custom transformation/Data Quality/ Reporting pipeline
      rdd
    }.foreachRDD(rdd=>{
      if (!rdd.isEmpty) {
        val df = sqlContext.read.schema(schema).json(rdd.map(x => x.value))
        val outputDF = dataFrame(df)
        outputDF.write.mode(SaveMode.Append).format("parquet").save(path)}
    })
    OffsetKeeper.commitKafkaOffsets(offsetRanges)
  }

}