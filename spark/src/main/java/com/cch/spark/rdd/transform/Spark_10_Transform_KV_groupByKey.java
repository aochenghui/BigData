package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_10_Transform_KV_groupByKey {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //TODO 准备数据
        JavaRDD<String> rdd = sc.textFile("data/word.txt");

        JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(v -> new Tuple2<>(v, 1));

        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
        groupRDD.collect().forEach(System.out::println);

        groupRDD
                .mapValues(iter -> iter.spliterator().estimateSize())
                .collect().forEach(System.out::println);

        sc.stop();

    }
}
