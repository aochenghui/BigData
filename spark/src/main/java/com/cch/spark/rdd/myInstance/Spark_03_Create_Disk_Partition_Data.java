package com.cch.spark.rdd.myInstance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark_03_Create_Disk_Partition_Data {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO Spark读取文件数据时
        //      分区数量的计算和分区数据的计算是不一样。
        //      Hadoop读取数据是按行读取，读一次，就读一行，不是按照字节读
        //          因为Hadoop主要目的是为了数据计算，而计算数据时，一般情况下，一行数据就是一条完整的业务数据，不能被拆分
        final JavaRDD<String> rdd = sc.textFile("data/word.txt", 3);

        rdd.saveAsTextFile("output");

        sc.stop();
    }
}
