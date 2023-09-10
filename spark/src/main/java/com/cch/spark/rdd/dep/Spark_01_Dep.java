package com.cch.spark.rdd.dep;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class Spark_01_Dep {
    public static void main(String[] args) throws InterruptedException {

        //TODO 2个阶段（分区数量：3） + 1个阶段（分区数量：2）
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> list = new ArrayList<>();
        list.add("zhangsan");
        list.add("lisi");
        list.add("wangwu");

        JavaRDD<String> rdd = sc.parallelize(list);

        // TODO RDD的依赖关系
        //    如果A用到了B，那么说明A依赖于B
        //
        final JavaRDD<String> flatMapRDD = rdd
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println( flatMapRDD.toDebugString() );

        System.out.println("******************************");

        final JavaPairRDD<String, Integer> mapRDD = flatMapRDD
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        System.out.println( mapRDD.toDebugString() );

        System.out.println("******************************");

        final JavaPairRDD<String, Integer> reduceRDD = mapRDD
                .reduceByKey(Integer::sum);
        System.out.println( reduceRDD.toDebugString() );

        System.out.println("******************************");

        reduceRDD.collect().forEach(System.out::println);

        sc.stop();
    }
}
