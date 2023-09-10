package com.cch.spark.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Spark_02_Action {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 7, 5, 3, 4, 1));

        // 1, 2, 3
        // 1, 5, 6

        // TODO 行动算子会触发作业（job）的执行
        //  多个行动算子触发的作业的个数：至少是行动算子的个数

        final long count = rdd.count();
        System.out.println(count);

        Integer first = rdd.first();
        System.out.println(first);

        rdd.collect();

        List<Integer> take = rdd.take(2);
        System.out.println(take);

        List<Integer> ordered = rdd.takeOrdered(3);
        System.out.println(ordered);

        Map<Integer, Long> stringLongMap = rdd.countByValue();
        System.out.println(stringLongMap);

        System.out.println(rdd.mapToPair(num -> new Tuple2<>(5, num)).countByKey());
        System.out.println(rdd.mapToPair(num -> new Tuple2<>(5, num)).countByValue());

        sc.stop();
    }
}
