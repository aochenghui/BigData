package com.cch.spark.rdd.sparkSql.jobs;

import org.apache.spark.sql.SparkSession;

public class SprakSQL_12_Test {
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

        sparkSession.sql("select\n" +
                "\t*\n" +
                "from (\n" +
                "\tselect\n" +
                "\t\t*,\n" +
                "\t\trank() over ( partition by area order by clickCnt desc ) rk\n" +
                "\tfrom (\n" +
                "\t\tselect\n" +
                "\t\t\tarea,\n" +
                "\t\t\tproduct_name,\n" +
                "\t\t\tcount(*) clickCnt\n" +
                "\t\tfrom (\n" +
                "\t\t\tselect\n" +
                "\t\t\t\t*\n" +
                "\t\t\tfrom user_visit_action\n" +
                "\t\t\twhere click_product_id != -1\n" +
                "\t\t) action\n" +
                "\t\tjoin (\n" +
                "\t\t\tselect\n" +
                "\t\t\t\tproduct_id,\n" +
                "\t\t\t\tproduct_name\n" +
                "\t\t\tfrom product_info\n" +
                "\t\t) p on action.click_product_id = p.product_id\n" +
                "\t\tleft join (\n" +
                "\t\t   select\n" +
                "\t\t\t   city_id,\n" +
                "\t\t\t   city_name,\n" +
                "\t\t\t   area\n" +
                "\t\t   from city_info\n" +
                "\t\t) c on action.city_id = c.city_id\n" +
                "\t\tgroup by area, product_id, product_name\n" +
                "\t) t\n" +
                ") t1 where rk <= 3").show();

        sparkSession.stop();
    }
}
