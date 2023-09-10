package com.cch.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class ConsumerGroupClient2 {

    public static void main(String[] args) {

        //1.先声明配置文件
        Properties properties = new Properties();
        //1.1 指定broker连接地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //1.2 对消息的key和value进行反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //1.3 指定当前消费者对应的组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group02");
        //TODO 指定分区策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
//        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        //2.获取客户端连接对象
        KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(properties);
        //3.订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("bigdata");
        topics.add("bigdata2");
        topics.add("second");
        kafkaConsumer.subscribe(topics);
        //4.拉取对应的消息
        while (true){
            ConsumerRecords<Object, Object> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Object, Object> record : records) {
                System.out.println(record);
            }
        }
    }
}
