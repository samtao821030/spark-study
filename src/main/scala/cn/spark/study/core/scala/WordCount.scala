package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2017/4/1 0001.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
          .setAppName("WordCountScala")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark1:9000/spark.txt",1)
    val result = lines.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    result.foreach(pair => println(pair._1+" appeared "+pair._2))
    sc.stop()
  }
}
