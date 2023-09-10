package com.cch.spark.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class Spark_07_Action_Test {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        final int[] sums = {0};

        // TODO 代码的执行位置
        //      rdd的方法之外的代码都是在Driver端执行的，Main方法的代码绝大多数都是在Driver端
        //      rdd的方法内部的函数代码是在Executor端执行的。
        //      如果rdd的方法内部的函数代码使用了外部的对象。那么这个对象必须要序列化
        rdd.foreach(
                num -> {
                    sums[0] = sums[0] + num;
                    System.out.println("Executor:" + sums[0]);
                }
        );

        // 10
        // 3
        // 7
        System.out.println("driver : " + sums[0]);


        sc.stop();
    }
}
