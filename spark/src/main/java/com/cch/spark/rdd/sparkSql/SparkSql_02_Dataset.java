package com.cch.spark.rdd.sparkSql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class SparkSql_02_Dataset {

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
//        Dataset<Long> mapDS = userDS.map(new MapFunction<User, Long>() {
//            @Override
//            public Long call(User value) throws Exception {
//                return value.getAge() + 100;
//            }
//        }, Encoders.LONG());
//        userDS.groupBy();
//        mapDS.show();

        // TODO 采用sql的方式
        //      表：table
        //      TempView：临时图，只对当前Session有效
        userDS.createOrReplaceTempView("user1");
        Dataset<Row> sqlDS = sparkSession.sql("select avg(age) from user1");
        sqlDS.show();

        sparkSession.stop();
    }
}
