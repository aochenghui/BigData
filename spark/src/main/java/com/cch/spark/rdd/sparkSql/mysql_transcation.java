package com.cch.spark.rdd.sparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class mysql_transcation {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("mysql_spark");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> json = spark.read().json("data/user.json");

        // 添加参数
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");

//        json.write()
//                // 写出模式针对于表格追加覆盖
//                .mode(SaveMode.Append)
//                .jdbc("jdbc:mysql://hadoop102:3306","gmall.testInfo",properties);

        Dataset<Row> jdbc = spark.read().jdbc("jdbc:mysql://hadoop102:3306", "mysql.db", properties);

        jdbc.show();
        json.show();


        //4. 关闭sparkSession
        spark.close();
    }
}
