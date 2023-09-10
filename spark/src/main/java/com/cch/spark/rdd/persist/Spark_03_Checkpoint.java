package com.cch.spark.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_03_Checkpoint {

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("HADOOP_USER_NAME", "cch");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setCheckpointDir("hdfs://hadoop102:8020/cp");

        JavaRDD<String> rdd = sc.textFile("data/word.txt");
        JavaRDD<String> flatMapRDD = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(s -> {
            System.out.println("##########");
            return new Tuple2<String, Integer>(s, 1);
        });

        // TODO 检查点
        //      Checkpoint directory has not been set in the SparkContext
        //      检查点设定的目的，就是将数据提供给多个应用程序。所以在底层会多执行一次作业将数据读取后保存下来。
        //      一般情况下，检查点是需要和缓存连用

        pairRDD.checkpoint();

        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(Integer::sum, 2);
        reduceRDD.collect().forEach(System.out::println);
        System.out.println("=============================================");

        pairRDD.collect().forEach(System.out::println);
        Thread.sleep(2222222);

        sc.stop();
    }

}
