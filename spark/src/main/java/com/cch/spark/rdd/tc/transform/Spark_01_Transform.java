package com.cch.spark.rdd.tc.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Spark_01_Transform {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> ss = new ArrayList<>();
        ss.add("zhangsna");
        ss.add("lisi");
        ss.add("wangwu");
        // parallelize方法获取到的RDD的功能：读取集合的数据
        final JavaRDD<String> rdd = sc.parallelize(ss);
        System.out.println(rdd);

        // 3. 组合计算对象
        //    Spark RDD其实就是分布式计算模型，但是为了考虑通用性，不能在一个RDD中封装所有的计算逻辑
        //    RDD中只是封装简单的计算逻辑，不同的RDD封装不同的逻辑，这样可以将多个不同的RDD组合在一起实现复杂的业务逻辑
        //    将多个RDD对象组合在一起的代码实现方式，其实就和IO处理方式完全一样，就是装饰者设计模式
        //    Java IO是咱们自己通过编码实现，但是RDD不是，是通过rdd方法实现。
        //    map : 变化（转换）
        //    RDD rdd1 = ....
        //    RDD rdd2 = rdd1.xxx()
        //    RDD的功能的组合是通过方法实现的，在Spark中有特殊的称呼：
        //    TODO 转换 + 算子(功能，方法)
        //    将RDD转换成一个新的RDD的方法（算子），存在两种不同方式
        //     这两种方式取决于数据的类型（格式）
        //     单值：1，2，3，zhangsan,lisi,wangwu
        //     键值(元组Tuple,两个元素组合在一起形成特殊结构，称之为二元组,也称之对偶)：(k, v),(k, v)

        // TODO 元组类型的作用
        //     多个无关元素的组合形成的特殊结构
        //     如果多个数据存在相同的含义，一般采用集合将数据封装起来。
        //     如果多个数据存在所属关系，一般采用对象进行封装。
        List<Integer> userids = new ArrayList<>();
        new Tuple2<String, String>("name", "zhangsan");
        new Tuple3<String, String, Integer>("zhangsan", "lisi", 123);

        final JavaRDD<Object> map = rdd.map(null);
        System.out.println(map);

//        final List<String> result = rdd.collect();
//        for (String s : result) {
//            System.out.println(s);
//        }
        sc.stop();

    }
}
