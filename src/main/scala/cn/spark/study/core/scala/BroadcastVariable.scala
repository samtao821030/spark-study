package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8 0008.
  */
object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3
    val numList = Array(1,2,3,4,5)

    val factor_broadcast = sc.broadcast(factor)

    val numRdd = sc.parallelize(numList).map(_*factor_broadcast.value)
    numRdd.foreach(println(_))

    sc.stop()
  }
}
