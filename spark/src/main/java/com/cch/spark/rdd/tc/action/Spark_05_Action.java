package com.cch.spark.rdd.tc.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark_05_Action {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2,3,4,5,6), 2);
        // TODO foreac方法将每个数据执行逻辑，有多少条数据，循环逻辑会执行多少遍。
//        rdd.foreach( num -> {
//            System.out.println("*************");
//        } );
        // TODO foreachPartition是将一个分区的数据全部加载到内存中传递给计算，进行处理
        //            以分区为单位进行计算，有多少个分区，循环逻辑会执行多少遍
        rdd.foreachPartition( iter -> {
            System.out.println("***********");

        } );

        sc.stop();


    }
}