package com.cch.spark.rdd.sparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Test10_Hive {


    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","cch");

        //1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");

        //2. 获取sparkSession
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()// 添加hive支持
                .config(conf).getOrCreate();

        //3. 编写代码
        spark.sql("show tables").show();

        //spark.sql("create table user_info(name String,age bigint)");
        spark.sql("insert into table user_info values('zhangsan',10)");
        spark.sql("select * from user_info").show();

        //4. 关闭sparkSession
        spark.close();
    }
}
