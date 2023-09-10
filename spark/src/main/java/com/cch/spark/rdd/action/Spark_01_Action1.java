package com.cch.spark.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Spark_01_Action1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> ss = new ArrayList<>();
        ss.add("zhangsan");
        ss.add("lisi");
        ss.add("wangwu");
        final JavaRDD<String> rdd = sc.parallelize(ss);

        rdd.map( name -> { return "name: " + name;})
                .collect().forEach(System.out::println);

        final JavaRDD<String> newRDD = rdd.sortBy(s -> s, true, 2);

        // TODO 转换算子和行动算子的区别：
        //      根据是否执行Job，如果执行，那么就是行动算子，如果不执行，就是转换,这句话是错误的。
        //      如果方法（算子）执行后返回的结果是RDD，就是转换算子
        //      如果方法（算子）执行后返回的具体的执行结果，就是行动算子
        final List<String> result = newRDD.collect();

        for (String s : result) {
            System.out.println(s);
        }

        sc.stop();
    }
}
