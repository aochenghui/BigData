package com.cch.spark.rdd.serial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class Spark_02_Serial {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //User要实现序列化接口
        ArrayList<User> list = new ArrayList<>();
        list.add(new User(1, "haha", 2));
        list.add(new User(2, "cch", 56));
        list.add(new User(3, "niu", 78));
        list.add(new User(4, "gg", 30));

        JavaRDD<User> rdd = sc.parallelize(list);
        JavaRDD<User> map = rdd.map(s -> s);
        map.collect().forEach(System.out::println);
        sc.stop();
    }
}
