package com.cch.spark.rdd.serial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;

public class Spark_03_Kryo {

    public static void main(String[] args) throws ClassNotFoundException {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore")
                // 替换默认的序列化机制
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // 注册需要使用kryo序列化的自定义类
                .registerKryoClasses(new Class[]{Class.forName("com.cch.spark.rdd.serial.User")});

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        User zhangsan = new User("zhangsan", 13);
        User lisi = new User("lisi", 13);
        User haha = new User("haha", 26);

        JavaRDD<User> userJavaRDD = sc.parallelize(Arrays.asList(zhangsan, lisi, haha), 2);

        JavaRDD<User> mapRDD = userJavaRDD.map(v1 -> new User(v1.getName(), v1.getScore() + 1));

        mapRDD. collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
