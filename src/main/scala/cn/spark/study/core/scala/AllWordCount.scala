package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/6 0006.
  */
object AllWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AllWordCount")
    val sc = new SparkContext(conf)
    val sum = sc.textFile("hdfs://spark1:9000/spark.txt").map(_.length).reduce(_ + _)
    println("The total num is:"+sum)
    sc.stop()
  }
}
