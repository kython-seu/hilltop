package base.learn

import org.junit.Test

import scala.collection.mutable

/**
  * Created by kason_zhang on 4/7/2017.
  */
class ScalaBase1 {

  /**
    * 循环测试，for while
    */
  @Test def LooperDemo(): Unit ={
    val array = Array(1,2,3,4)
    var i = 0
    while (i < array.length){
      println(array(i))
      i = i + 1
    }
  }

  /**
    * 函数文本
    */
  @Test def LopperForeach(): Unit ={
    val array = Array(1,2,3,4)

    array.foreach((ele : Int) => println(ele))

    array.foreach(ele => println(ele))

    array.foreach(println)
  }

  /**
    * Tuple元祖
    */
  @Test def tupleTest(): Unit ={
    val tuple = (2, "hello Tuple")
    println(tuple._1)
    println(tuple._2)

    val (fir,_) = tuple
    println(fir)
  }

  /**
    * Set and Map
    * 可变 以及 不可变
    */
  @Test def setAndMap(): Unit ={

    var muset = mutable.Set(1,2,3)
    println(muset)
    muset.add(4)
    println(muset)


    var map = mutable.Map("lili" -> 14, "lucy" -> 21, "lilei" -> 30)
    for((key,value) <- map){
      println("key is " + key +" value is " + value)
    }
    map += ("hehe" -> 29)
    for((key,value) <- map){
      println("key is " + key +" value is " + value)
    }
  }
}
