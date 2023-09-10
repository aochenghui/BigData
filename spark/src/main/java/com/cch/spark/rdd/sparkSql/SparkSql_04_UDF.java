package com.cch.spark.rdd.sparkSql;

import org.apache.spark.sql.*;

public class SparkSql_04_UDF {

    public static void main(String[] args) {

        final SparkSession sparkSession = SparkSession
                .builder()  //构建器
                .master("local[*]")
                .appName("SparkSql")
                .getOrCreate();

        // TODO SparkSql中使用的新的分布式数据处理模型，底层封装的其实还是RDD
        Dataset<Row> jsonDS = sparkSession.read().json("data/user.json");
       jsonDS.createOrReplaceTempView("user");


        sparkSession.stop();
    }
}
