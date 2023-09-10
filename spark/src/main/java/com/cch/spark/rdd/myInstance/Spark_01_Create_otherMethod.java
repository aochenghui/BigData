package com.cch.spark.rdd.myInstance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Spark_01_Create_otherMethod {
    public static void main(String[] args) {

        // TODO Spark的代码的编写步骤

        new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("SparkTest")).parallelize(new ArrayList<String>(Arrays.asList("zhangsan","lisi","wangwu"))).collect().forEach(System.out::println);

    }
}
