package cn.spark.study.sql.java;

import cn.spark.study.bean.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;

import java.util.List;

/**
 * Created by Administrator on 2017/4/13 0013.
 */
public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lineRdd = sc.textFile(RDD2DataFrameReflection.class.getResource("/students.txt").getPath());
        JavaRDD<Student> studentRDD = lineRdd.map(new Function<String, Student>() {
            @Override
            public Student call(String line) throws Exception {
                String[] lineArray = line.split(",");
                Integer id = Integer.parseInt(lineArray[0]);
                String name = lineArray[1];
                Integer age = Integer.parseInt(lineArray[2]);
                Student student = new Student();
                student.setId(id);
                student.setName(name);
                student.setAge(age);
                return student;
            }
        });

        DataFrame studentDF = sqlContext.createDataFrame(studentRDD,Student.class);
        studentDF.registerTempTable("students");
        DataFrame teenagerDF = sqlContext.sql("select id,name,age from students where age<18");
        JavaRDD<Row> rows = teenagerDF.toJavaRDD();
        JavaRDD<Student> resultRDD = rows.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Integer id = row.getInt(0);
                String name = row.getString(1);
                Integer age = row.getInt(2);
                Student student = new Student();
                student.setId(id);
                student.setName(name);
                student.setAge(age);
                return student;
            }
        });

        List<Student> students = resultRDD.collect();
        for(Student student:students){
            System.out.println(student);
        }
    }
}
