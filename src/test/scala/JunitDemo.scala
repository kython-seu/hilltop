import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.Test
import org.junit.Assert._
/**
  * Created by kason_zhang on 4/6/2017.
  */
class JunitDemo {

  val conf : SparkConf = new SparkConf().setAppName("test").setMaster("local")
  val sc : SparkContext = new SparkContext(conf)
  @Test def test1(): Unit ={
    val s = sc.parallelize(Array(1,2,3,4,5)).reduce((x,y) => x + y)
    println(s)
    assertEquals(15,s)
  }

  /**
    * 统计单次
    */
  @Test def calculateWordnum(): Unit ={
    var result = sc.textFile("D:\\work\\cloud\\test.txt").flatMap(str => str.split(" ")).map(str => (str,1))
      .reduceByKey((x,y) => x + y)
    //println(result.collect())
    for( i <- result.collect()){
      println(i)
    }

  }

  /**
    * test union
    */
  @Test def unionTest(): Unit ={
    val aa_ = sc.textFile("D:\\work\\cloud\\test.txt").filter(str => str.contains("aa"))
    val bbb_ = sc.textFile("D:\\work\\cloud\\test.txt").filter(str => str.contains("bbb"))
    val re = aa_.union(bbb_)
    re.foreach(println)

  }

  /**
    * fold aggregate
    */
  @Test def fold_aggregate(): Unit ={
    val data  = Array(1,2,3,4)
    val re = sc.parallelize(data).fold(0)((x,y)=>x+y)
    println(re)

    var rdd1 = sc.makeRDD(1 to 10,2)

  }

  /**
    * 键值对的操作
    */

  /**
    * 将第一个单词作为键
    */
  @Test def turn(): Unit ={
    val file = sc.textFile("D:\\work\\cloud\\test.txt")
    val re = file.map(str => (str.split(" ")(0),str))

    println(re.collect().foreach(println))

    //Pair RDD单个的转化操作
    val data = sc.parallelize(Array((1,2),(3,4),(3,6))) //得到{(1,2),(3,6),(3,6)}的RDD
    val reduce_by_key = data.reduceByKey((x,y) => x + y)
    println(reduce_by_key.count())
    println(reduce_by_key.collect().foreach(println))


    println(data.groupByKey().collect().foreach(println))

    println(data.mapValues(x => x + 1).collect().foreach(println))

    println(data.flatMapValues(x => x to 5).collect().foreach(println))

    
  }

  /**
    * Spark累加器共享变量
    */
  @Test def accu(): Unit ={
//    val acc = sc.longAccumulator("Long Accumulator")
//    val data = sc.parallelize(Array(1,2,3,4))
//    data.foreach(x => {
//      println(acc.value)
//      acc.add(x)
//    })
//
//    println("count is " + data.count())
//    println(acc.value)
//    println("count is " + data.count())
//    println(acc.value)
    /**
      * 不可靠的累加器
      */
    val acc = sc.accumulator(0)
    val lines = sc.textFile("D:\\work\\cloud\\test.txt")
    val callsigns = lines.flatMap(line => {
      //println(acc.value)
      acc += 1
      line.split(" ")
    })
    //callsigns.saveAsTextFile("D:\\work\\cloud\\output.txt")
    callsigns.count()
    println(acc.value)
    callsigns.count()
    println(acc.value)
    //callsigns.saveAsTextFile("D:\\work\\cloud\\output2.txt")
    //println(acc.value)
    /**
      * 可靠的累加器
      */
      val acc_ = sc.accumulator(0)
    val kekao = lines.foreach(str => {

      acc_ += 1
      println("line is " + str)
    })
    lines.count()
    println(acc_.value)

  }
}
