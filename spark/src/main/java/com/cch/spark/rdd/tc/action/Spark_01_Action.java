package com.cch.spark.rdd.tc.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Spark_01_Action {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> ss = new ArrayList<>();
        ss.add("zhangsna");
        ss.add("lisi");
        ss.add("wangwu");
        final JavaRDD<String> rdd = sc.parallelize(ss);

        final JavaRDD<String> newRDD = rdd.map(
                name -> {
                    System.out.println("###########");
                    return "name " + name;
                }
        );

        // TODO RDD是一个计算模型，可以采用特殊的代码编写方式将多个RDD的功能组合在一起
        //      但是组合是不能执行的。只有调用相应的方法才能让RDD的计算开始执行。
        //      collect就是真正执行的方法

        // 延迟加载（执行）
        System.out.println("*************************");
        // TODO Action方法决定了数据处理结果该如何进行后续的处理
        //      collect: 将RDD的数据处理结果采集到调度节点，形成了一个结果集合
//        final List<String> result = newRDD.collect();
//        for (String s : result) {
//            System.out.println(s);
//        }
        sc.stop();

        /*
            // TODO 延迟加载（执行） lazy
           list = test1() // **********
           test2() // ##########
           println(list)


         */

    }
}
class User {

    public void test() {
        System.out.println("***********");
    }

    public static void main(String[] args) {
        System.out.println("################");
    }

}
