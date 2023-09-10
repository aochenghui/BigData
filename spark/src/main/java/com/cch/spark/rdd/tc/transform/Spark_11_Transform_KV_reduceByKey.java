package com.cch.spark.rdd.tc.transform;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;

public class Spark_11_Transform_KV_reduceByKey {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 准备键值对数据
        final JavaRDD<String> rdd = sc.textFile("data/word.txt");

        final JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(
                        word -> new Tuple2<>(word, 1)
                );

        // hello, hello, hello
        // (hello,1),(hello,1),(hello,1)
        // TODO reduceByKey方法可以根据相同key对v进行聚合，基本的聚合逻辑就是两两聚合
//        pairRDD.reduceByKey(
//                (v1, v2) -> v1 + v2
//        ).collect().forEach(System.out::println);

        pairRDD.reduceByKey(Integer::sum).collect().forEach(System.out::println);

        sc.stop();

    }
}
