package com.cch.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class HBase01_Connection {
    public static void main(String[] args) throws IOException {
        testConnection();
    }

    /**
     * 同步的Connection
     */
    public static void testConnection() throws IOException{
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103, hadoop104");
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println(connection);

        connection.close();
    }

    /**
     * 异步的Connection
     */
    public static void testConnection1() throws ExecutionException, InterruptedException, IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103, hadoop104");
        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(configuration);
        System.out.println(asyncConnection.get());

        asyncConnection.get().close();
    }
}