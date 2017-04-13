package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8 0008.
  */
object WordCountSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountSort").setMaster("local")
    val sc = new SparkContext(conf)
    val resultRDD = sc.textFile(WordCountSort.getClass.getResource("/spark.txt").getPath).flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    resultRDD.foreach(x=>{
      println(x._1+":"+x._2)
    })
  }
}
