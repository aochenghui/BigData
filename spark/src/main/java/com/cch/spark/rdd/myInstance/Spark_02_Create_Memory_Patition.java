package com.cch.spark.rdd.myInstance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class Spark_02_Create_Memory_Patition {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //  final JavaRDD<String> rdd = sc.parallelize(Arrays.asList("zhangsan", "lisi", "wangwu","12","hj","hsh","hhh","niuniu","last"));
        // 将管道对象对接数据源时，同时设定管道的数量
        // Spark是基于Hadoop开发的，所以读取文件，Spark是没有能力，只能使用底层的Hadoop来读取文件
        // Hadoop读取文件时，一般称之为切片
        // 分区数据的存储其实是依靠底层算法的，这个算法类似于kafka的算法
        // Kafka 算法：平均分，如果不能平均分，向前补齐
        // Spark 算法：平均分，如果不能平均分，向后补齐

        // TODO 为了查看RDD对象中分区处理过程，可以采用特殊的方法
        //      saveAsTextFile方法可以将不同分区的数据保存到不同的（分区）文件中
        //      分区数量默认取值为当前的环境中虚拟核数
        //      如果不想使用环境默认的核数（分区），可以自行设定
        //      自行设定，需要给parallelize方法传递第二个参数：分区数

//        final JavaRDD<Integer> rdd_4 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 4);
//        rdd_4.saveAsTextFile("output_4");
//
//        final JavaRDD<Integer> rdd_6 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 6);
//        rdd_6.saveAsTextFile("output_6");
//
//        final JavaRDD<Integer> rdd_8 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 8);
//        rdd_8.saveAsTextFile("output_8");
//
//        final JavaRDD<Integer> rdd_86 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 6);
//        rdd_86.saveAsTextFile("output_8_6");

        /*
                def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
                     (0 until numSlices).iterator.map { i =>
                        val start = ((i * length) / numSlices).toInt
                        val end = (((i + 1) * length) / numSlices).toInt
                        (start, end)
                    }
                }
         */
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 7);

        final JavaRDD<Integer> rdd1 = rdd.map( v1 -> v1 * 2);

        rdd1.saveAsTextFile("output_7");
        sc.stop();

    }
}
