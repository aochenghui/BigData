package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark_05_Transform_filter {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> stringRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6),2);

        // TODO filter : 按照指定的规则对分区数据进行过滤
        //        data -> true(false), true的数据保留，false的数据丢弃

        // TODO 特殊情况下，过滤后的数据可能会产生数据倾斜，需要特殊处理。
        stringRDD
            .filter( num -> num % 2 == 0 )
            .collect().forEach(System.out::println);


        sc.stop();

    }
}
