package com.cch.spark.rdd.tc.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark_04_Action {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2,3,4,5,6), 2);

        // TODO
        //    forEach方法是 List的方法, List是单点的
        //    foreach方法是 RDD的方法,  RDD是分布式的

        // collect将数据采集回到Driver端的时候，采用的方式按照分区顺序采集的
        final List<Integer> collect = rdd.collect();
        collect.forEach(System.out::println);
        System.out.println("*****************************");
        rdd.foreach( num -> {
            System.out.println(num);
        } );

        sc.stop();


    }
}