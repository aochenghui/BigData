package com.cch.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 开发模式：
 * 1、 面向客户端开发
 * -- 获取客户端连接对象
 * -- 调用具体API完成功能
 * -- 关闭连接
 *
 *  TODO 同步发送
 */
public class ProducerClientSync {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //声明一个配置对象
        Properties properties = new Properties();
        //指定broker的连接地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //指定发送的消息的key和value的序列化对象
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //获取客户端连接对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //调用send()进行消息传送
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "cch-" + i))
                    .get();     //同步发送
        }

        //关闭资源
        kafkaProducer.close();
    }
}
