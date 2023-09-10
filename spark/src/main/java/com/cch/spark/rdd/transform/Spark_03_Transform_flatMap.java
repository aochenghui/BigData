package com.cch.spark.rdd.transform;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class Spark_03_Transform_flatMap {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> oldRDD = sc.parallelize(Arrays.asList("hello world", "hadoop scala", "java flink", "gaga go", "cc python"), 2);

        // TODO 将数据字符串中每一个单词独立出来
        //   "hello world" => "hello", "world"
        //      map功能核心作用： 1k -> 1v
        //      实现不了    ：   1k -> 1v + 1v + 1v
        //   如果将一个数据拆分成多个数据来使用，这个过程称之为扁平化：flatten
        //   将整体拆分成个体的操作就是扁平化操作，获取的结果就是一个一个的个体
        //   Spark RDD提供了一个方法，又可以将数据扁平化，同时还可以转换。flatMap
//        final JavaRDD<String[]> newRDD = oldRDD.map(s -> s.split(" "));
//        final JavaRDD<String> newRDD = oldRDD.flatMap( s -> {
//            final String[] s1 = s.split(" ");
//            for (int i = 0; i < s1.length; i++) {
//                s1[i] = "test:" + s1[i];
//            }
//            return new ArrayIterator(s1);
//              return Arrays.asList(s.split(" ")).iterator();
//        } );

        JavaRDD<String> newRDD = oldRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaRDD<String> newRDD1 = newRDD.flatMap(s->{
            String[] split = s.split(" ");
            for (int i = 0; i < split.length; i++) {
                split[i] = "test:" + split[i] ;
            }
            return new ArrayIterator(split);
        });
        newRDD1.collect().forEach(System.out::println);
        sc.stop();
    }
}
