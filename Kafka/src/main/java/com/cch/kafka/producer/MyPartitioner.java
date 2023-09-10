package com.cch.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {

    /**
     * 自定义分区规则的核心方法
     * 需求：发送过来的数据中如果包含cch，就往0号分区，包含niuniu，就往1号分区，都不包含，就往2号分区发送
     */
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String data = o1.toString();
        int partitions;
        if (data.contains("cch")){
            partitions = 0;
        }else if (data.contains("niuniu")){
            partitions = 1;
        }else {
            partitions = 2;
        }
        return partitions;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
