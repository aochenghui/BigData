package com.cch.spark.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_02_Persist {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("data/word.txt");
        JavaRDD<String> flatMapRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(s -> {
            System.out.println("##########");
            return new Tuple2<String, Integer>(s, 1);
        });

        // TODO 缓存：内存
//        pairRDD.cache();
        // TODO 持久化：默认取值和cache是一样的
        pairRDD.persist(StorageLevel.DISK_ONLY());

        final JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(Integer::sum, 2);
        reduceRDD.collect().forEach(System.out::println);

        System.out.println("*****************************************");

        pairRDD.saveAsTextFile("output");


        Thread.sleep(2222222);

        sc.stop();
    }

}
