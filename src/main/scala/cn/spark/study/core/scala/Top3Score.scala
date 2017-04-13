package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8 0008.
  */
object Top3Score {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)
    val resultRDD = sc.textFile(Top3.getClass.getResource("/score.txt").getPath)
      .map(x=>{
        val className = x.split(" ")(0)
        val score = x.split(" ")(1)
        (className,score)
    })
      .groupByKey()
        .map(x=>{
          val className=x._1
          val scores = x._2
          val scoreArray = scores.toArray
          val sortedArray = scoreArray.sortWith((a,b)=>a>b)
          val scoreString = sortedArray.slice(0,3).mkString(" ")
          (className,scoreString)
        })

    resultRDD.foreach(x=>{println(x._1+":"+x._2)})

  }
}
