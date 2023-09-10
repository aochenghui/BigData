package com.cch.spark.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_02_Transform_map1 {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> oldRDD = sc.parallelize(Arrays.asList(1,2,3,4),2);

        //TODO map方法就是用于将旧的RDD功能进行补充，形成新的RDD
        //     map方法补充的功能：可以将流转数据进行一个一个的转换， A -> B
        //    Integer => Integer

        // 将面向对象的编程方式简化为函数式编程方式
        // 1. 匿名类
//        final JavaRDD<Integer> newRDD = oldRDD.map(new Function<Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1) throws Exception {
//                return v1 * 2;
//            }
//        });
        // 2. 注解，给类增加标记，在编译时，编译器会根据注解进行特殊编译
        //    @FunctionalInterface 注解就是告诉编译器，当前的接口实现可以采用特殊的函数式编程语法实现
//        final JavaRDD<Integer> newRDD = oldRDD.map(
//                (v1) -> {return v1 * 2;}
//        );
        //   JDK1.8的语法来自于Scala语言，Scala语言有一个非常重要的原则：能省则省
        //   如果参数的个数只有一个，此时，参数列表的小括号可以省略
        final JavaRDD<Integer> newRDD = oldRDD.map(
                v1 -> {return v1 * 2;}
        );
        //   如果代码逻辑中只有一行代码，那么大括号，分号,return可以省略
        final JavaRDD<Integer> map = oldRDD.map(v1 -> v1 * 2);
        //   如果参数在逻辑代码中只使用了一次，还可以简化
        //final JavaRDD<Integer> map1 = oldRDD.map(NumberOperate::mul2);

        newRDD.saveAsTextFile("output");

        sc.stop();

    }
}
//class NumberOperate {
//    public static Integer mul2( Integer v ) {
//        return v * 2;
//    }
//}