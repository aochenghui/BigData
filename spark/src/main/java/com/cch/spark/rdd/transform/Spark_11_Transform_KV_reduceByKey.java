package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_11_Transform_KV_reduceByKey {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //TODO 准备键值对数据
        JavaRDD<String> rdd = sc.textFile("data/word.txt");

        JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(v -> new Tuple2<>(v, 1));

        JavaPairRDD<String, Integer> rdd1 = pairRDD.reduceByKey(Integer::sum);
        rdd1.collect().forEach(System.out::println);

        sc.stop();

    }
}
