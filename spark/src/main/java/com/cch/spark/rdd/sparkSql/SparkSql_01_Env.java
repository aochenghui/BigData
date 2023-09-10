package com.cch.spark.rdd.sparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
public class SparkSql_01_Env {

    public static void main(String[] args) {
        // TODO SparkSQL其实就是Spark RDD的一个简化版本
        //    所以SparkSQL进行了很多的封装，包括环境对象
//         SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL");
//         JavaSparkContext jsc = new JavaSparkContext(conf);
        // TODO SparkSession其实就是SparkSQL的环境对象
        //      类似于JDBC中Connection
        //      设计模式：构建器Builder模式
//        final SparkSession sparkSession = new SparkSession(jsc.sc());

//        sparkSession.sql("show tables").show();

        SparkSession sparkSession = SparkSession
                .builder()  //构建器
                .master("local[*]")
                .appName("SparkSql")
                .getOrCreate();

        sparkSession.stop();
    }
}
