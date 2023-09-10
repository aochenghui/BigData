package com.cch.spark.rdd.tc.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Spark_03_Create_Disk_Partition {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 将文件作为数据源时，分区和当前环境无关。
        //      Spark是没有读取文件的能力，基于Hadoop开发的，所以读取文件底层采用的是Hadoop实现的
        //      所以当前我们的分区数量其实就是hadoop读取文件时的切片数量
        /*
         textFile 方法存在第二个参数，表示最小分区数


           分区规则：
                平均分
                如果不能平均分的时候，会判断余数是否超过分区容量的10%，
                    如果超过10%，那么新增一个分区（切片）
                    如果没有超过10%，那么就不会新增一个分区（切片）
           totalSize : 6 byte
           goalSize  : 6 / 3 = 2

           10%

           7 - 3 * 2 = 1
           分区数量 = 4

         */
        final JavaRDD<String> rdd = sc.textFile("data/word.txt", 3);

        rdd.saveAsTextFile("output");

        sc.stop();

    }
}
