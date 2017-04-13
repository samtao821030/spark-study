package cn.spark.study.sql.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by Administrator on 2017/4/13 0013.
 */
public class DataFrameCreateJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameCreateJava");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("hdfs://spark1:9000/students.txt");
        df.show();
        df.printSchema();
        df.select("name").show();
    }
}
