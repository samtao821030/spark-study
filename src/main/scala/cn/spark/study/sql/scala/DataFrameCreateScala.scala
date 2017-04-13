package cn.spark.study.sql.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/13 0013.
  */
object DataFrameCreateScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameCreateScala")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://spark1:9000/students.txt")
    df.show()
  }
}
