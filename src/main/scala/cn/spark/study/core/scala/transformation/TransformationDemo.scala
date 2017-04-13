package cn.spark.study.core.scala.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/7 0007.
  */
object TransformationDemo {
  def map(sc:SparkContext)={
    val numbers = Array(1,2,3,4,5,6)
    val numbersRDD = sc.parallelize(numbers).map(_*2)
    numbersRDD.foreach(println(_))
    sc.stop()
  }

  def filter(sc:SparkContext)={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers).filter(_%2==0)
    numbersRDD.foreach(println(_))
    sc.stop()
  }

  def groupByKey(sc:SparkContext)={
    val scoreList = List(("class1",80),("class2",75),("class1",90),("class2",65))
    val scoreRDD = sc.parallelize(scoreList).groupByKey(1)
    scoreRDD.foreach(score=>{
      val className = score._1
      val scores = score._2
      scores.foreach(score=>{print(className+" "+score)
        println()
      })
    })
  }

  def reduceByKey(sc:SparkContext)={
    val scoreList = List(("class1",80),("class2",75),("class1",90),("class2",65))
    val scoreRDD = sc.parallelize(scoreList).reduceByKey((v1,v2)=>v1+v2)
    scoreRDD.foreach(score=>{println(score._1+" "+score._2)})
  }

  def sortByKey(sc:SparkContext)={
    val scoreList = List(("lilei",80),("wanghong",75),("haozi",90),("chenchen",65))
    val scoreRDD = sc.parallelize(scoreList).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    scoreRDD.foreach(score=>{println(score._1+" "+score._2)})
  }

  def join(sc:SparkContext)={
    val studentList = List((1,"leo"),(2,"jack"),(3,"tom"))
    val scoreList = List((1,100),(2,90),(3,60),(1,70),(2,80),(3,60))
    val studentRDD = sc.parallelize(studentList)
    val scoreRDD = sc.parallelize(scoreList)
    val resultRDD = studentRDD.join(scoreRDD)
    resultRDD.foreach(x=>{
      println("id:"+x._1)
      val result = x._2
      println("name:"+result._1)
      println("score:"+result._2)
      println()
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
          .setAppName("MapDemo")
            .setMaster("local")
    val sc = new SparkContext(conf)

    //map(sc)
    //filter(sc)
    //groupByKey(sc)
    //reduceByKey(sc)
    //sortByKey(sc)
    join(sc)
  }

}
