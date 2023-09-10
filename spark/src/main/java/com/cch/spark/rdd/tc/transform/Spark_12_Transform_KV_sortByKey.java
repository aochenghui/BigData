package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_12_Transform_KV_sortByKey {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 准备键值对数据
        final JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 3),
                        new Tuple2<>("b", 1),
                        new Tuple2<>("a", 1),
                        new Tuple2<>("b", 2),
                        new Tuple2<>("a", 2),
                        new Tuple2<>("b", 3)
                )
        );

        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 4, 3, 5, 2));


        // TODO sortByKey方法：根据key对数据进行排序，默认为升序
        // (a, [3,1,2]) => (a, [1,2,3])
        // (b, [1,2,3]) => (b, [1,2,3])
        pairRDD.sortByKey(false).collect().forEach(System.out::println);
        // 1,4,3,5,2
        // ("1",1),("4",4),("3",3),("5",5)("2",2)
        rdd.sortBy(num -> "" + num, true, 2).collect().forEach(System.out::println);

        sc.stop();

    }
}
