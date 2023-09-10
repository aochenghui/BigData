package com.cch.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 开发模式：
 * 1、 面向客户端开发
 * -- 获取客户端连接对象
 * -- 调用具体API完成功能
 * -- 关闭连接
 *
 * TODO 带回调函数
 */
public class ProducerClientAsyncCallBack {

    public static void main(String[] args) throws InterruptedException {
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
        for (int i = 0; i < 7; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("bigdata2", i,"hah","cch-" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println("主题: " + recordMetadata.topic()
                                + " 分区: " + recordMetadata.partition()
                                + " 发送时间: " + recordMetadata.timestamp());
                    }else {
                        e.printStackTrace();
                    }
                }
            });

            //线程休眠
//            Thread.sleep(1);
        }
        //关闭资源
        kafkaProducer.close();
    }
}
