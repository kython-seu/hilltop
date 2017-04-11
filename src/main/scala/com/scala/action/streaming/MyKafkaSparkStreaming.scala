package com.scala.action.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kason_zhang on 4/11/2017.
  */
object MyKafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyKafkaStreamingDemo").setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val topicLines = KafkaUtils.createStream(ssc,"10.64.24.78:2181"
      ,"StreamKafkaGroupId",Map("spark" -> 1))
    topicLines.map(_._2).flatMap(str => str.split(" ")).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
