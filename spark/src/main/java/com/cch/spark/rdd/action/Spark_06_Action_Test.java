package com.cch.spark.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class Spark_06_Action_Test {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        Emp emp = new Emp();
        rdd.foreach( num -> System.out.println(emp.age + num));

        sc.stop();
    }
}

class Emp implements Serializable{
    public int age = 30;
}
