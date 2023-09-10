package com.cch.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * @author HUIHUI
 *
 * 胖客户端
 *
 * 基于标准的JDBC
 */
public class PhoenixThickDemo {

    public static void main(String[] args) {
        testUpsert();
        testSelect();
    }
    /**
     * 查询
     *
     * 标准的JDBC的编码步骤:
     *   注册驱动  获取连接  编写SQL  编译SQL  设置参数  执行SQL  封装结果  关闭连接
     */
    public static void testSelect(){
        try {
            //注册驱动
            //可以省略不写， 会按照url自动推断要连的数据库， 也就能推断对应的Driver
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181" ;
            //获取连接
            Properties properties = new Properties();
            properties.put("phoenix.schema.isNamespaceMappingEnabled" , "true") ;
            Connection connection = DriverManager.getConnection(url , properties);
            //设置事务是否自动提交
            //connection.setAutoCommit(true / false );

            //编写SQL
            String sql ="select id , name, age from student where id = ? " ;
            //编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            //设置参数
            preparedStatement.setString(1 , "1002");
            //执行SQL
            ResultSet resultSet = preparedStatement.executeQuery();// executeUpdate()
            //封装结果
            if(resultSet.next()){
                String id  = resultSet.getString("id");
                String name = resultSet.getString("name");
                long age = resultSet.getLong("age") ;
                System.out.println(id  + " : " + name  + " : " + age  );
            }
            //关闭连接
            resultSet.close();
            preparedStatement.close();
            connection.close();

            //connection.commit();

        } catch (Exception e) {
            //connection.rollback();
            throw new RuntimeException(e);
        }
    }

    /**
     * 事务操作：
     *    1. 将事务设置为自动提交， 不需要人管理
     *         connection.setAutoCommit(true / false );
     *           Mysql的事务默认为自动提交。
     *           Phoenix的事务默认为不自动提交。
     *
     *    2. 手动控制事务
     *         connection.commit(); 没有异常，提交事务
     *         connection.rollback(); 出现异常，回滚事务
     *
     *
     * 作业:
     * 添加、修改
     */
    public static void testUpsert(){
        try {
            //注册驱动
            //可以省略不写， 会按照url自动推断要连的数据库， 也就能推断对应的Driver
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181" ;
            //获取连接
            Properties properties = new Properties();
            properties.put("phoenix.schema.isNamespaceMappingEnabled" , "true") ;
            Connection connection = DriverManager.getConnection(url , properties);
            //设置事务是否自动提交
            //connection.setAutoCommit(true / false );

            //编写SQL
            String sql ="upsert into student (id, name, age, addr) values(? , ? , ?, 'beijing') " ;
            //编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            //设置参数
            preparedStatement.setString(2 , "niuniu");
            preparedStatement.setString(1 , "1002");
            preparedStatement.setLong(3 , 25L);
            //执行SQL
            int i = preparedStatement.executeUpdate();// executeUpdate(
            System.out.println(i + "行已经更改!!");
            //关闭连接
            preparedStatement.close();
            connection.close();

            //connection.commit();

        } catch (Exception e) {
            //connection.rollback();
            throw new RuntimeException(e);
        }
    }

    /**
     * 作业:
     * 删除
     */
    public static void testDelete(){
        try {
            //注册驱动
            //可以省略不写， 会按照url自动推断要连的数据库， 也就能推断对应的Driver
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181" ;
            //获取连接
            Properties properties = new Properties();
            properties.put("phoenix.schema.isNamespaceMappingEnabled" , "true") ;
            Connection connection = DriverManager.getConnection(url , properties);
            //设置事务是否自动提交
            //connection.setAutoCommit(true / false );

            //编写SQL
            String sql ="delete id , name, age from student where id = ? " ;
            //编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            //设置参数
            preparedStatement.setString(1 , "1002");
            //执行SQL
            int t = preparedStatement.executeUpdate();// executeUpdate()
            System.out.println(t + "行被删除!!!");
            //关闭连接
            preparedStatement.close();
            connection.close();

            //connection.commit();

        } catch (Exception e) {
            //connection.rollback();
            throw new RuntimeException(e);
        }
    }

}
