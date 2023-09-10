package com.cch.spark.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_01_Persist {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("data/word.txt");
        JavaRDD<String> flatMapRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(Integer::sum, 2);
        reduceRDD.collect().forEach(System.out::println);

        System.out.println("======================================================");

        JavaRDD<String> rdd1 = sc.textFile("data/word.txt");
        final JavaRDD<String> flatMapRDD1 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        final JavaPairRDD<String, Integer> mapRDD1 = flatMapRDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        mapRDD1.saveAsTextFile("output");

        Thread.sleep(2222222);
    }

}
