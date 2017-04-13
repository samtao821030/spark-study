package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/6 0006.
  */
object ParallellizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
            .setAppName("ParallellizeCollection")
              .setMaster("local")

    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numrdd = sc.parallelize(numbers,5)
    val sum = numrdd.reduce(_ + _)
    print("数据的总和为"+sum)
  }
}
