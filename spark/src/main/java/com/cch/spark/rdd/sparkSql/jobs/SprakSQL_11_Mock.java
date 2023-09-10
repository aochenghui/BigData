package com.cch.spark.rdd.sparkSql.jobs;

import org.apache.spark.sql.SparkSession;

public class SprakSQL_11_Mock {
    public static void main(String[] args) {

        // TODO 1. 程序中如果要操作HDFS，默认采用windows登录用户
        System.setProperty("HADOOP_USER_NAME","cch");

        // TODO 2. 启用Hive的支持
        final SparkSession sparkSession = SparkSession
                        .builder()
                        .enableHiveSupport()
                        .master("local[*]")
                        .appName("SparkSQL")
                        .getOrCreate();

        //sparkSession.sql("create database if not exists sparksql ");
        sparkSession.sql("use sparksql");

        // TODO 创建表
//        sparkSession.sql("CREATE TABLE `user_visit_action`(\n" +
//                "  `date` string,\n" +
//                "  `user_id` bigint,\n" +
//                "  `session_id` string,\n" +
//                "  `page_id` bigint,\n" +
//                "  `action_time` string,\n" +
//                "  `search_keyword` string,\n" +
//                "  `click_category_id` bigint,\n" +
//                "  `click_product_id` bigint, --点击商品id，没有商品用-1表示。\n" +
//                "  `order_category_ids` string,\n" +
//                "  `order_product_ids` string,\n" +
//                "  `pay_category_ids` string,\n" +
//                "  `pay_product_ids` string,\n" +
//                "  `city_id` bigint --城市id\n" +
//                ")\n" +
//                "row format delimited fields terminated by '\\t';");
//
//        sparkSession.sql("load data local inpath '/opt/module/spark-3.3.1-yarn/jobs/user_visit_action.txt' into table user_visit_action");
//
//        sparkSession.sql("CREATE TABLE `product_info`(\n" +
//                "  `product_id` bigint, -- 商品id\n" +
//                "  `product_name` string, --商品名称\n" +
//                "  `extend_info` string\n" +
//                ")\n" +
//                "row format delimited fields terminated by '\\t';");
//
//        sparkSession.sql("load data local inpath 'data/product_info.txt' into table product_info");
//
//        sparkSession.sql("CREATE TABLE `city_info`(\n" +
//                "  `city_id` bigint, --城市id\n" +
//                "  `city_name` string, --城市名称\n" +
//                "  `area` string --区域名称\n" +
//                ")\n" +
//                "row format delimited fields terminated by '\\t';");
//
//        sparkSession.sql("load data local inpath 'data/city_info.txt' into table city_info");

//        sparkSession.sql("select * from city_info limit 10").show();
        sparkSession.sql("select * from user_visit_action limit 10").show();

        sparkSession.stop();
    }
}
