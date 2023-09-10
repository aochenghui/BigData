package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark_09_Transform_KV_mapValues_1 {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 准备键值对数据
        final JavaRDD<String> rdd = sc.textFile("data/word.txt");

        // hello, hadoop, hive, spark
        // (hello, 1), (hadoop, 1)
        // 如果获取的数据为单值类型，需要转换键值类型时，可以采用特殊的方法
        final JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(
                        word -> new Tuple2<>(word, 1)
                );


        // TODO mapValues方法：可以将数据保持K不变的情况下，对Value进行转换处理
        pairRDD.mapValues(Object::toString).collect().forEach(System.out::println);


        sc.stop();

    }
}
