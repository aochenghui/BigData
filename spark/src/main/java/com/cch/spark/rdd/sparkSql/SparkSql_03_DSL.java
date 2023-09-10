package com.cch.spark.rdd.sparkSql;

import org.apache.spark.sql.*;

public class SparkSql_03_DSL {

    public static void main(String[] args) {

        final SparkSession sparkSession = SparkSession
                .builder()  //构建器
                .master("local[*]")
                .appName("SparkSql")
                .getOrCreate();

        // TODO SparkSql中使用的新的分布式数据处理模型，底层封装的其实还是RDD
        Dataset<Row> jsonDS = sparkSession.read().json("data/user.json");
        // 从文件中获取整形数据类型都是BigInt，在转换对象属性时，属性应该采用Long类型声明
        Dataset<User> userDS = jsonDS.as(Encoders.bean(User.class));

        // TODO 使用函数进行处理的时候，会出现类型冲突的情况
        //      在使用时，需要明确类型
        // TODO DSL语法
        // select name from user
        //userDS.select("name", "age").show();
        userDS.select(functions.col("name").as("newName")).show();

        sparkSession.stop();
    }
}
