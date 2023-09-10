package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark_06_Transform_distinct {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> stringRDD = sc.parallelize(Arrays.asList(1,1,2,2,3,3),3);

        // TODO distinct 方法用于将数据进行去重
        //      RDD的distinct方法是分布式去重,采用的是分组聚合的方式，数字本身就是分组的标记
        //      (1, [1,1])
        //      (2, [2,2])  => [1,2,3]
        //      (3, [3,3])

        stringRDD
            .distinct(2)
            .collect().forEach(System.out::println);


        sc.stop();

    }
}
