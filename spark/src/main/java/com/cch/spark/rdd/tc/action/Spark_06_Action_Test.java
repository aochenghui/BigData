package com.cch.spark.rdd.tc.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class Spark_06_Action_Test {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(1,2,3,4), 2);

        Emp emp = new Emp();

        rdd.foreach(
            num -> {
                System.out.println( emp.age + num );
            }
        );


        sc.stop();


    }
}
class Emp implements Serializable {
    public int age = 30;
}