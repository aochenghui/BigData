package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark_04_Transform_groupBy_2 {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> stringRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6),3);
        /*

                        /-----1,2,3 ----> [0, (2,4,6)]
          [1,2,3,4,5,6]
                        \-----4,5,6 ----> [1, (1,3,5)]


         */
        // 在组合多个RDD时，默认分区数量不会改变的。
        // 分组后的数据如何存储的呢
        // 1. Spark数据在分组后必须要保证一个组的数据在一个分区，不能跨越分区的
        //        一个分区中不见得只有一个组
        // 2. Spark数据在分组时，打乱之前的默认分区规则，将数据进行重新组合，这个操作，在Spark中有特殊的称呼：shuffle

        stringRDD
            //.groupBy( num -> num % 2, 2 )
            .groupBy( num -> num % 2,2 )
            .collect().forEach(System.out::println);
            //.saveAsTextFile("output");

        Thread.sleep(999999999);

        sc.stop();

    }
}
