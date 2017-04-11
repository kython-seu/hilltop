package com.scala.action.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kason_zhang on 4/6/2017.
  */
object ApiBasic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("API BASIC").setMaster("local");
    val sc = new SparkContext(conf)
    var fileRdd = sc.textFile("D:\\work\\cloud\\test.txt")
    println("myapp "+fileRdd.count())

    val array = Array(1,2,3,4,5)
    val distData = sc.parallelize(array)
  }
}
