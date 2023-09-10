package com.cch.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 封装单例Connection
 */
public class Hbase02_SingletonConnection {

    private static Connection connection = null;

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103, hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
        }catch (IOException e){
            throw new RuntimeException();
        }
    }

    public static Connection getConnection(){
        return connection;
    }

    public static void closeConnection(){
        if (connection != null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
