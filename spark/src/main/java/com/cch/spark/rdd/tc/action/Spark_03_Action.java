package com.cch.spark.rdd.tc.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_03_Action {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2,3,4,5,6), 2);

        //rdd.saveAsTextFile("output");
        rdd.saveAsObjectFile("output1");

        sc.stop();


    }
}