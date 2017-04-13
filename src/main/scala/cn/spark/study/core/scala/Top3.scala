package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8 0008.
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)
    val resultRDD = sc.textFile(Top3.getClass.getResource("/top.txt").getPath).sortBy(x=>{x.toInt},false,1)
    val top3 = resultRDD.take(3)
    top3.foreach(println(_))
  }
}
