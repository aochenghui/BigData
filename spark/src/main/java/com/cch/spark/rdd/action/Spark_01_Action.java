package com.cch.spark.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class Spark_01_Action {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> ss = new ArrayList<>();
        ss.add("zhangsan");
        ss.add("lisi");
        ss.add("wangwu");
        final JavaRDD<String> rdd = sc.parallelize(ss);

        JavaRDD<String> newRDD = rdd.map(name -> {
            System.out.println("#########################");
            return "name " + name;
        });

        System.out.println("************************");

        newRDD.collect().forEach(System.out::println);

    }
}
