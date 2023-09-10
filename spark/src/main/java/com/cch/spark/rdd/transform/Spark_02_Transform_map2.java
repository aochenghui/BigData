package com.atguigu.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark_02_Transform_map2 {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> oldRDD = sc.parallelize(Arrays.asList(1,2,3,4),2);

        // TODO 并行计算，分区
        /*
         1 3 2 4
         3 1 4 2
         3 4 1 2
         1 2 3 4

         规律1 : 数字1在数字2之前打印，数字3在数字4之前打印
         规律2 : 数字1和数字3不确定谁先打印
         -------------------------------
         分区内有序，分区间无序

         */
        final JavaRDD<Integer> newRDD = oldRDD.map(v1 -> {
            System.out.println("@@@@@@@" + v1);
            return v1;
        });
        final JavaRDD<Integer> newRDD1 = newRDD.map(v1 -> {
            System.out.println("#######" + v1);
            return v1;
        });


        //newRDD1.saveAsTextFile("output3");
        newRDD1.collect().forEach( System.out::println );

        sc.stop();

    }
}