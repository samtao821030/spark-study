package cn.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;


/**
 * 本地测试的WordCount程序
 * Created by Administrator on 2017/4/1 0001.
 */
public class WordCountLocal {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local"); //使用setMaster()可以设置Spark应用程序要连接spark集群的master节点url;如果设置为local代表在本地运行
        //创建JavaSparkContext对象
        //在Spark中,SparkContext是Spark所有程序的一个入口,主要作用是初始化一些核心组件,包括调度器(DAGScheduler,TaskScheduler),并且在Master节点上进行注册
        //编写不同类型的应用程序,需要不同的Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        //根据输入源,创建初始的RDD
        JavaRDD<String> lines = sc.textFile(WordCountLocal.class.getResource("/spark.txt").getPath());
        //对初始RDD进行算子操作
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        //需要以单词作为key获得单词的频率
        JavaPairRDD<String,Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //执行foreach,触发action操作
        wordcounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" appeared "+wordCount._2);
            }
        });

        sc.close();
    }
}
