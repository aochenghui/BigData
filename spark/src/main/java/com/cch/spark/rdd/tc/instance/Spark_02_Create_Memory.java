package com.cch.spark.rdd.tc.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.ParallelCollectionRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Spark_02_Create_Memory {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 将内存的数据作为数据源，将数据管道对象进行对接
        //      需要采用parallelize方法
        //      方法返回的就是具体的管道对象（RDD）: ParallelCollectionRDD
        //      Scala语言可以不遵循类的转换关系，因为底层，由编译器帮助我们自动完成了转换
        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList("zhangsan", "lisi", "wangwu"));
        final JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4));
        System.out.println(rdd1);

        final List<String> result = rdd.collect();
        for (String s : result) {
            System.out.println(s);
        }
        sc.stop();

    }
}
