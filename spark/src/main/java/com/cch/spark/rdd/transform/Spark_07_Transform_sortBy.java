package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark_07_Transform_sortBy {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> stringRDD = sc.parallelize(Arrays.asList(1,15,6,24,13,22),3);

        // TODO sortBy : 按照指定的规则对数据进行排序
        //      sortBy方法提供三个参数：
        //           第一个参数为：排序的规则 数据 -> 标记
        //           第二个参数为：排序的方式：true为升序，false为降序
        //           第三个参数为：分区数量（底层存在shuffle）
        // "1","15","6","24","13“,”22“
        // "6", "24", "22", "15", "13", "1"
        // 1(1),15(1),6(0),24(0),13(1),22(0)
        // sortBy底层采用的是特殊算法(采样算法)实现分区操作。存在shuffle。
        // 1 ~ 10000, true, 3
        // 1, 10,202,3054, 7760,40
        // (1, 10)(10~40)(40-202)(202-3054)(3054,7760)
        // (1 ~ 3054)(3054~ 7760)(7760 ~10000)
        stringRDD
            .sortBy( num -> num + "", false, 3 ) // 24, 22,15,13,6,1
            .collect().forEach(System.out::println);

        sc.stop();

    }
}
