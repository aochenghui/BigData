package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark_03_Transform_flatMap_1 {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        final JavaRDD<String> oldRDD = sc.parallelize(Arrays.asList("hello world", "hadoop hive spark", "flink"),2);
//        final JavaRDD<String> newRDD = oldRDD.flatMap( s -> Arrays.asList(s.split(" ")).iterator());
//        final List<String> result = newRDD.collect();
//        result.forEach(System.out::println);

        // 函数式编程推荐的写法
//        sc
//            .parallelize(Arrays.asList("hello world", "hadoop hive spark", "flink"),2)
//            .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//            .collect()
//            .forEach(System.out::println);

        // 如果某一步操作的结果需要重复利用， 那么应该将结果返回，使用变量进行引用。这个引用不要随便改。
//        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello world", "hadoop hive spark", "flink"),2);
//        final JavaRDD<String> rdd1 = rdd.flatMap( s -> Arrays.asList(s.split(" ")).iterator());
//        final List<String> result = rdd1.collect();
//        result.forEach(System.out::println);
//        System.out.println("***************************************");
//        rdd.map( s -> "string:" + s ).collect().forEach(System.out::println);

        final JavaRDD<String> oldRDD = sc.parallelize(Arrays.asList("hello world", "hadoop hive spark", "flink"),2);
        final JavaRDD<String> newRDD1 = oldRDD.flatMap( s -> Arrays.asList(s.split(" ")).iterator());
        final JavaRDD<String> newRDD2 = newRDD1.flatMap( s -> Arrays.asList(s.split(" ")).iterator());

        // map -> flatMap -> new map -> new flatMap
        // 功能在组合时，是不可以重复使用的，但是重复使用后，其实产生的是新的功能和RDD，而不是之前的RDD
        // 在Spark的执行过程中，可以使用一种特殊的拓扑图形进行表示（DAG:有向无环图）
        // 拓扑图 : 点和线形成的几何图形
        //        spark中点就表示功能
        //        spark中线就表示数据的流转方向
        //        spark数据流转不允许从后续的功能流转到前面的功能
        final List<String> result = newRDD2.collect();
        result.forEach(System.out::println);

        //Thread.sleep(9999999);


        sc.stop();
    }
}
