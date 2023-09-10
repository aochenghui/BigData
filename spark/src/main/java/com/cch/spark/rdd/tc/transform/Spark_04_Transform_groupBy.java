package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark_04_Transform_groupBy {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> stringRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6),2);

        // TODO groupBy : 通过什么规则对数据进行分组
        //      group : 组
        //      By : 通过
        //    RDD的功能不能太复杂，需要将多个RDD的功能组合在一起
        //    flatMap + groupBy
        //    groupBy分组规则：给数据增加一个标记，相同标记的数据会放置在一个组里
        stringRDD
            //.flatMap( s -> Arrays.asList(s.split(" ")).iterator() )
            //.groupBy( num -> num % 2 == 0 )
            .groupBy( num -> num % 2 )
            .collect().forEach(System.out::println);


        sc.stop();

    }
}
