package com.cch.spark.rdd.dep;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class Spark_02_Dep {
    public static void main(String[] args) throws InterruptedException {

        //TODO 2个阶段（分区数量：3） + 1个阶段（分区数量：2）
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> list = new ArrayList<>();
        list.add("zhangsan");
        list.add("lisi");
        list.add("wangwu");

        JavaRDD<String> rdd = sc.parallelize(list, 3);
        JavaRDD<String> flatMapRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapRDD = flatMapRDD.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey(Integer::sum, 2);
        reduceRDD.collect().forEach(System.out::println);

        Thread.sleep(9999999);
        sc.stop();
    }
}
