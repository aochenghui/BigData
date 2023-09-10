package com.cch.spark.rdd.combat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkCore {

    public static void main(String[] args) {

        // TODO 建立连接
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkCombat");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 1.读取数据
        //        Spark没有读取文件数据的能力，只能依靠hadoop
        //        Hadoop读取文件是一行一行读取的
        final JavaRDD<String> fileRDD = sc.textFile("data/user_visit_action.txt");

        // TODO 2.将数据进行筛选过滤，保留有效，合法数据
        //         filter方法返回结果的类型应该为布尔类型：true表示数据有效，需要保留，反之，数据无效，应该丢弃
        JavaRDD<String> dataRDD = fileRDD.filter(line -> {
            String[] ss = line.split("_");
            return "null".equals(ss[5]);
        });

        // TODO 3. 过滤后的数据的格式：一行完整用户行为数据
        //         需要进行统计的数据格式：（品类,(点击数量，下单数量，支付数量)）
        //         将过滤后的数据格式转换为需要进行统计的数据格式
        //       3.1 如果是点击行为数据(1) -> (1)那么可以直接获取品类
        //       3.2 如果是下单行为数据(1) -> (N)那么不能直接获取品类，但是可以分解品类数据
        //       3.3 如果是支付行为数据(1) -> (N)那么不能直接获取品类，但是可以分解品类数据
        JavaRDD<CategoryCount> formatRDD = dataRDD.flatMap(
                line -> {
                    String[] ss = line.split("_");
                    if (!"-1".equals(ss[6])) {
                        // TODO 点击行为数据
                        return Arrays.asList(new CategoryCount(Integer.parseInt(ss[6]), 1, 0, 0)).iterator();
                    } else if (!"null".equals(ss[8])) {
                        // TODO 下单行为数据
                        ArrayList<CategoryCount> ccs = new ArrayList<>();
                        for (String cid : ss[8].split(",")) {
                            ccs.add(new CategoryCount(Integer.parseInt(cid), 0, 1, 0));
                        }
                        return ccs.iterator();
                    } else if (!"null".equals(ss[10])) {
                        // TODO 支付行为数据
                        ArrayList<CategoryCount> ccs = new ArrayList<>();
                        for (String cid : ss[10].split(",")) {
                            ccs.add(new CategoryCount(Integer.parseInt(cid), 0, 0, 1));
                        }
                        return ccs.iterator();
                    } else {
                        return new ArrayList<CategoryCount>().iterator();
                    }
                }
        );

        // TODO 4. 将转换格式后的数据进行统计聚合
        //    (鞋, (1, 0, 0))
        //    (鞋, (0, 1, 0))
        //    (鞋, (1, 0, 0))
        //    (鞋, (0, 0, 1))
        //    ---------------
        //    (鞋, (2, 1, 1))

        final JavaPairRDD<Integer, CategoryCount> pairRDD = formatRDD.mapToPair(
                c -> new Tuple2<>(c.getCid(), c)
        );

        final JavaPairRDD<Integer, CategoryCount> reduceRDD = pairRDD.reduceByKey(
                (c1, c2) -> {
                    c1.setClickCount(c1.getClickCount() + c2.getClickCount());
                    c1.setOrderCount(c1.getOrderCount() + c2.getOrderCount());
                    c1.setPayCount(c1.getPayCount() + c2.getPayCount());
                    return c1;
                }
        );

        // TODO 5.对统计的结果进行排序
        final JavaRDD<CategoryCount> vRDD = reduceRDD.map(
                kv -> kv._2
        );

        final JavaRDD<CategoryCount> sortRDD = vRDD.sortBy( v1 -> v1, false,3);

        // TODO 6.取前10名
        sortRDD.take(10).forEach(System.out::println);

        sc.stop();
    }
}

class CategoryCount implements Serializable,Comparable<CategoryCount> {

    private Integer cid;
    private Integer clickCount;
    private Integer orderCount;
    private Integer payCount;

    public CategoryCount() {}

    public CategoryCount(Integer cid, Integer clickCount, Integer orderCount, Integer payCount) {
        this.cid = cid;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public Integer getCid() {
        return cid;
    }

    public void setCid(Integer cid) {
        this.cid = cid;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }

    public Integer getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Integer orderCount) {
        this.orderCount = orderCount;
    }

    public Integer getPayCount() {
        return payCount;
    }

    public void setPayCount(Integer payCount) {
        this.payCount = payCount;
    }

    @Override
    public String toString() {
        return  "cid=" + cid +
                ", clickCount=" + clickCount +
                ", orderCount=" + orderCount +
                ", payCount=" + payCount ;
    }

    @Override
    public int compareTo(CategoryCount cate) {
        if ( this.clickCount > cate.clickCount ) {
            return 1;
        } else if ( this.clickCount < cate.clickCount ) {
            return -1;
        } else {
            if ( this.orderCount > cate.orderCount ) {
                return 1;
            } else if (this.orderCount < cate.orderCount) {
                return -1;
            } else {
                return this.payCount - cate.payCount;
            }
        }
    }
}