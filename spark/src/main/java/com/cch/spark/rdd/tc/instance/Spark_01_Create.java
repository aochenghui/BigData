package com.cch.spark.rdd.tc.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Spark_01_Create {
    public static void main(String[] args) throws Exception {

        // TODO Spark的代码的编写步骤
        // 1. 创建Spark框架的环境对象，因为需要通过环境对象获取Spark框架的计算相关的对象
        //    构建环境时，需要指定环境配置
        //    RDD对象不能直接new，因为如果直接new，无法进行分布式计算。
        //    所以需要通过环境对象获取指定的RDD对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. 获取计算相关对象(RDD)
        //    建立数据源和数据处理管道（RDD）之间的关系
        //    数据源：1. 内存 2. 磁盘， 3. MySQL, Hive
        //    parallelize方法就是用于建立内存数据和RDD的关联
        //    JavaRDD<String> :
        //       JavaRDD : 分布式计算模型（对象），类似于数据管道
        //       <String> : 分布式计算时，数据流转的类型是什么
        //   final : 最终的
        //      final String s = "zhangsan"; // 不可变变量
        //      "zhangsan"， 123， true, 'A'; // 常量
        List<String> ss = new ArrayList<String>();
        ss.add("zhangsna");
        ss.add("lisi");
        ss.add("wangwu");
        final JavaRDD<String> rdd = sc.parallelize(ss);

        // 3. 组合计算对象

        // 4. 执行计算
        //    组合计算对象，其实就是组合计算功能，将简单的计算功能组合在一起形成复杂的功能
        //    但是，组合操作并不能让数据流转起来。必须通过特定的操作让数据流转起来。
        final List<String> result = rdd.collect();
        for (String s : result) {
            System.out.println(s);
        }

        // 4.5 计算过程如果时间很长，那么在执行过程中，是可以查看监控
        //     需要采用特定的端口进行查看：4040
        //Thread.sleep(9999999);

        // 5. 计算完毕，释放资源
        sc.stop();

    }
}
