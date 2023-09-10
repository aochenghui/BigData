package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_09_Transform_KV_mapValues1 {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //TODO 准备数据
        JavaRDD<String> rdd = sc.textFile("data/word.txt");

        //hello,hadoop,hive,spark
        //(hello, 1), (hadoop, 1)

        JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1));

        pairRDD.mapValues(Object :: toString).collect().forEach(System.out::println);
        sc.stop();

    }
}
