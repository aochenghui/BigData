package com.cch.spark.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark_04_Action {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);

        // TODO
        //    forEach方法是 List的方法, List是单点的
        //    foreach方法是 RDD的方法,  RDD是分布式的

        /*
            collect将数据采集回到Driver端的时候，采用的方式按照分区顺序采集的
         */
        // TODO
        List<Integer> collect = rdd.collect();
        collect.forEach(System.out::println);

        System.out.println("*****************************************");
        // TODO
        rdd.foreach(num -> { System.out.println(num);});
        sc.stop();
    }
}
