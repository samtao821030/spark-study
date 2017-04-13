package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8 0008.
  */
object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSort").setMaster("local")
    val sc = new SparkContext(conf)
    val resultRDD = sc.textFile(SecondSort.getClass.getResource("/sort.txt").getPath)
                .map(x=>{
                  val first = x.split(" ")(0).toInt
                  val second = x.split(" ")(1).toInt
                  val secondKey = new SecondarySortKey(first,second)
                  (secondKey,x)
                })
                  .sortByKey()
                  .map(x=>{x._2})

    resultRDD.foreach(println(_))

  }
}
