package com.cch.spark.rdd.sparkSql.jobs;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;
import scala.Serializable;

import java.util.*;

public class SprakSQL_12_Test_2 {
    public static void main(String[] args) {

        // TODO 1. 程序中如果要操作HDFS，默认采用windows登录用户
        System.setProperty("HADOOP_USER_NAME","cch");

        // TODO 2. 启用Hive的支持
        final SparkSession sparkSession =
                SparkSession
                        .builder()
                        .enableHiveSupport()
                        .master("local[*]")
                        .appName("SparkSQL")
                        .getOrCreate();

        // TODO 操作Hive
        sparkSession.sql("use sparksql");

        // TODO TopN的实现思路
        //     需求：各区域中热门商品Top3
        //        排序：sort desc(降序)
        //        热门：点击量
        //        统计：（商品，点击量）
        /*
           word count

           华北   鞋   100
           华北   衣服 100
           华北   鞋   200
           东北   鞋   300
           华北   鞋   400

            // TODO 统计聚合时，数据会变少 : group by
           （（华北,鞋）， 700）
           （（东北,鞋）， 300）
           （（华北,衣服），100）

           组内排序
           // TODO 组内排序，数据不会变少 : 开窗函数（加标记）
           华北 ：
                （鞋，   700）
                （衣服， 100）


         */
        // 1. 查询用户行为数据（search,click,order,pay）
        //    (多什么，删什么) 将数据进行筛选过滤，保留点击数据

        // 2. （缺什么，补什么）关联城市信息表和商品信息表，补全数据
        //   补全数据：补全行（union），补全列(join)

        // 3. 根据区域和商品ID进行分组聚合（点击数量）
        // 4. 根据区域进行分组，组内进行点击数量的排序（降序），并增加标记（行号）
        // 5. 根据行号进行筛选过滤，保留前三名。

        /*
TODO : 实现思路：
         */
        // 1. 查询数据
        sparkSession.sql("select\n" +
                "\t*\n" +
                "from (\n" +
                "\tselect\n" +
                "\t\t*\n" +
                "\tfrom user_visit_action\n" +
                "\twhere click_product_id != -1\n" +
                ") action\n" +
                "join (\n" +
                "    select\n" +
                "\t\tproduct_id,\n" +
                "\t\tproduct_name\n" +
                "\tfrom product_info\n" +
                ") p on action.click_product_id = p.product_id\n" +
                "left join (\n" +
                "   select\n" +
                "       city_id,\n" +
                "\t   city_name,\n" +
                "\t   area\n" +
                "   from city_info\n" +
                ") c on action.city_id = c.city_id").createOrReplaceTempView("t1");

        // 2. 根据区域和商品ID进行分组聚合（点击数量）
        sparkSession.udf().register("cityRemark", functions.udaf( new CityRemarkUDAF(), Encoders.STRING()));
        sparkSession.sql("select\n" +
                "    area,\n" +
                "\tproduct_name,\n" +
                "cityRemark(city_name) remark," +
                "\tcount(*) clickCnt\n" +
                "from t1\n" +
                "group by area, product_id, product_name").createOrReplaceTempView("t2");

        // 3. 根据区域进行分组，组内进行点击数量的排序（降序），并增加标记（行号）
        sparkSession.sql("select\n" +
                "\t*,\n" +
                "\trank() over ( partition by area order by clickCnt desc ) rk\n" +
                "from t2").createOrReplaceTempView("t3");

        // 4. 根据行号进行筛选过滤，保留前三名。
        sparkSession.sql("select\n" +
                "\t*\n" +
                "from t3 where rk <= 3").show(false);

        sparkSession.stop();
    }
}
class CityRemarkUDAF extends Aggregator<String, CityRemarkBuffer,String> {
    @Override
    public CityRemarkBuffer zero() {
        return new CityRemarkBuffer(0, new HashMap<String, Integer>());
    }

    @Override
    public CityRemarkBuffer reduce(CityRemarkBuffer buffer, String city) {
        buffer.setTotal(buffer.getTotal() + 1);
        final Map<String, Integer> cityMap = buffer.getCityMap();
        final Integer oldCnt = cityMap.getOrDefault(city, 0);
        cityMap.put(city, oldCnt + 1);

        return buffer;
    }
    // [（北京，10），（天津，5）]， [（北京，5），（郑州，3）]
    // [（北京，15），（天津，5），（郑州，3）]
    @Override
    public CityRemarkBuffer merge(CityRemarkBuffer b1, CityRemarkBuffer b2) {
        b1.setTotal(b1.getTotal() + b2.getTotal());
        final Map<String, Integer> cityMap1 = b1.getCityMap();
        final Map<String, Integer> cityMap2 = b2.getCityMap();
        final Iterator<String> keyIter2 = cityMap2.keySet().iterator();
        while ( keyIter2.hasNext() ) {
            final String key2 = keyIter2.next();
            final Integer v2 = cityMap2.get(key2);
            final Integer v1 = cityMap1.get(key2);
            if ( v1 == null ) {
                cityMap1.put(key2, v2);
            } else {
                cityMap1.put(key2, v1 + v2);
            }
        }

        return b1;
    }

    @Override
    public String finish(CityRemarkBuffer buffer) {

        StringBuilder ss = new StringBuilder();

        final Integer total = buffer.getTotal();
        final Map<String, Integer> cityMap = buffer.getCityMap();

        List<CityCount> ccs = new ArrayList<>();
        cityMap.forEach(
                (k, v) -> {
                    ccs.add(new CityCount(v,k));
                }
        );
        Collections.sort(ccs);

        int rest = 100;
        int i = 0;
        for (CityCount cc : ccs) {
            if ( i == 2 ) {
                break;
            }
            //ss.append(cc.getCity() + " " + String.format("%.2f", 1.0 * cc.getCount() / total) +"%");
            int r = cc.getCount() * 100 / total;
            rest -= r;
            ss.append(cc.getCity() + " " + r +"%, ");
            i++;
        }

        if ( ccs.size() > 2 ) {
            ss.append( "其他 " + rest +"%");
        }

        return ss.toString();
    }

    @Override
    public Encoder<CityRemarkBuffer> bufferEncoder() {
        return Encoders.kryo(CityRemarkBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}
class CityCount implements Serializable, Comparable<CityCount> {
    private Integer count;
    private String city;

    public CityCount(Integer count, String city) {
        this.count = count;
        this.city = city;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public int compareTo(@NotNull CityCount other) {
        return other.count - this.count;
    }
}
