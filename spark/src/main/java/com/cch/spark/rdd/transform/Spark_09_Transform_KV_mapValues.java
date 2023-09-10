package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark_09_Transform_KV_mapValues {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //TODO 准备数据
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("zhangsan", 30),
                new Tuple2<>("lisi", 40),
                new Tuple2<>("wangwu", 50)
        );

        // Spark默认情况下不会将Tuple2当成键值对
        // 如果想要让Spark可以识别键值数据，必须采用其他特殊的方法
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);

        rdd.mapValues(Object::toString).collect().forEach(System.out::println);
    }
}
