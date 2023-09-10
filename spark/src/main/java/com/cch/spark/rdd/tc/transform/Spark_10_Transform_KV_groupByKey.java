package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class Spark_10_Transform_KV_groupByKey {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 准备键值对数据
        final JavaRDD<String> rdd = sc.textFile("data/word.txt");

        final JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(
                        word -> new Tuple2<>(word, 1)
                );


        // TODO groupByKey方法：分组规则是固定的，固定采用Key作为分组标记
        //                     相同的key将V放置在一个组中
        // hello, hello, hello
        // (hello,1),(hello,1),(hello,1)
        // (hello, [1,1,1])
        // =>
        // (hello, 3)
        final JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
        groupRDD
                .mapValues( iter -> iter.spliterator().estimateSize() )
                .collect().forEach(System.out::println);

        // TODO groupBy方法其实底层实现就是groupByKey
        //     hello, hive, hadoop = (h, [hello, hive, hadoop])
        //     hello, hive, hadoop => (h, hello)(h, hive)(h,hadoop) => (h, [hello, hive,hadoop])
        //pairRDD.groupBy()



        sc.stop();

    }
}
