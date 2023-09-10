package com.cch.spark.rdd.serial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class Spark_01_Serial {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> list = new ArrayList<>();
        list.add("zhangsan");
        list.add("lisi");
        list.add("wangwu");
        final JavaRDD<String> rdd = sc.parallelize(list);

        // TODO RDD算子（方法）
        // rdd算子外部的代码都是在Driver端执行的
        // rdd算子内部的代码都是在Executor端执行的
        // rdd算子内部的代码如果使用了外部代码的对象，那么必须保证使用的对象能够序列化

        rdd.map(s -> s).collect().forEach(System.out::println);

        sc.stop();
    }
}
