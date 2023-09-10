package com.cch.spark.rdd.myInstance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Spark_03_Create_Disk {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 可以将磁盘文件作为数据源
        //      通过textFile方法对象文件数据源
        //      创建的管道对象为 : MapPartitionsRDD
        //      文件路径(具体文件，文件夹)：
        //      绝对路径：
        //      相对路径：IDEA的配置路径
        final JavaRDD<String> rdd = sc.textFile("data/word.txt");

        List<String> resullt = rdd.collect();
        for (String s : resullt) {
            System.out.println(s);
        }

        sc.stop();
    }
}
