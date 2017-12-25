package com.scala.action.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kason_zhang on 4/10/2017.
  */
object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("WordCountStreaming")
    val ssc = new StreamingContext(conf,Seconds(2))
    val lines = ssc.textFileStream("D:\\work\\cloud\\test.txt")
    val words_count = lines.flatMap(str => str.split(" "))
      .map(x => (x,1))
      .reduceByKey((x, y) => x + y)
    words_count.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
