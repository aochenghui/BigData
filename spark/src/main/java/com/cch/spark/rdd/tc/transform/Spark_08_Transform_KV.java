package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_08_Transform_KV {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO Spark的RDD用于对数据进行分布式计算，提供了很多相应的功能方法
        //      但是这些方法会根据数据格式的不同，而实现不同的逻辑
        //      单值：1，2，3，zhangsan,lisi List【1，2，3】
        //      键值：List【（k,v），(k1,v1),(k2,v2)】
        //           采用了特殊数据类型 Tuple(元组：多个无关元素的组合，称之为元组)
        new Tuple2<String, Integer>("zhangsan", 30);


        sc.stop();

    }
}
