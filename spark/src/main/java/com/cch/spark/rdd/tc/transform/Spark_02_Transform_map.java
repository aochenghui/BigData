package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class Spark_02_Transform_map {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> oldRDD = sc.parallelize(Arrays.asList(1,2,3,4),2);

        //TODO map方法就是用于将旧的RDD功能进行补充，形成新的RDD
        //     map方法补充的功能：可以将流转数据进行一个一个的转换， A -> B
        //    Integer => Integer
        final JavaRDD<Integer> newRDD = oldRDD.map( new MapFunction() );

        newRDD.saveAsTextFile("output");


        sc.stop();

    }
}
class MapFunction implements Function<Integer, Integer> {
    @Override
    public Integer call(Integer v) throws Exception {
        return v * 2;
    }
}
