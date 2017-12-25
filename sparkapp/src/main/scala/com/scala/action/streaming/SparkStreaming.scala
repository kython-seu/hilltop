package com.scala.action.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kason_zhang on 4/7/2017.
  */
object SparkStreaming {

  def main(args: Array[String]): Unit = {
    /*//构造SparkConf
    val conf = new SparkConf().setMaster("local").setAppName("Streaming")
    //根据SparkConf 构建StreamingContext并指定了1s的批处理大小
    val sparkStreamingContext = new StreamingContext(conf,Seconds(5))
    //链接到本地机器7777端口上，使用收到的数据创建DStream
    val lines = sparkStreamingContext.rawSocketStream("localhost",20000)
    //根据DStream进行处理，本例进行筛选出hello的行
    println("heheh")
    //lines.print()

    //val hellolines = lines.filter(str => str.contains("hello"))

    //val hellolines = lines.print()
    //hellolines.print()
    println("haha")
    //hellolines.saveAsTextFiles("D:\\work\\cloud\\demo.txt")
    sparkStreamingContext.start()

    sparkStreamingContext.awaitTermination()*/

//    if (args.length < 2) {
//      System.err.println("Usage BasicStreamingExample <master> <output>")
//    }
//    val Array(master, output) = args.take(2)

    val conf = new SparkConf().setMaster("local[3]").setAppName("BasicStreamingExample")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("10.64.24.78" , 9999)
    val words = lines.flatMap(_.split(" "))
    val wc = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    wc.print
    wc.saveAsTextFiles("D:\\work\\cloud\\log\\word.txt")
    println("pandas: sscstart")
    ssc.start()
    println("pandas: awaittermination")
    ssc.awaitTermination()
    println("pandas: done!")
  }

}
