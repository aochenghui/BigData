package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Spark_02_Transform_map {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> oldRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
        // TODO map方法就是用于将旧的RDD功能进行补充，形成新的RDD
        //      map方法补充的功能：可以将流转数据进行一个一个的转换，A -> B
        //      Integer => Integer
        JavaRDD<Integer> newRDD = oldRDD.map( v1 -> v1 * 2);
        newRDD.collect().forEach(System.out::println);

        sc.stop();
    }
}
