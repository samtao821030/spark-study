package cn.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8 0008.
  */
object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local")
    val sc = new SparkContext(conf)
    val accu_value = sc.accumulator(0)
    val numList = Array(1,2,3,4,5)
    val numRDD = sc.parallelize(numList)
    numRDD.foreach(value=>{
      accu_value.add(1)
    })
    println("最终结果为:"+accu_value.value)
  }
}
