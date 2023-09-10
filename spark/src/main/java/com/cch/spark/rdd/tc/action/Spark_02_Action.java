package com.cch.spark.rdd.tc.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Spark_02_Action {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1,1,1,1));

        // 1, 2, 3
        // 1, 5, 6
        //final JavaRDD<Integer> newRDD = rdd.map(name -> name);

        // TODO 行动算子会触发作业（job）的执行
        //     多个行动算子触发的作业的个数:至少是行动算子的个数
//        final long count = newRDD.count();
//        System.out.println(count);
//        final String first = newRDD.first();
//        System.out.println(first);
//        newRDD.collect();
//        final List<String> take = newRDD.take(2);
//        final List<Integer> strings = newRDD.takeOrdered(3);
//        System.out.println(strings);
//        final Map<Integer, Long> stringLongMap = newRDD.countByValue();
//        System.out.println(stringLongMap);
        System.out.println(rdd.mapToPair(num -> new Tuple2<>(5, num)).countByKey());

        sc.stop();


    }
}