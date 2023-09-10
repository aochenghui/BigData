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
 * TODO 生产者的分区器对象
 *      --1. 测试1 指定分区编号的结果
 */
public class ProducerClientAsyncCallBackPartitioner {

    public static void main(String[] args) throws InterruptedException {
        //声明一个配置对象
        Properties properties = new Properties();
        //指定broker的连接地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //指定发送的消息的key和value的序列化对象
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // TODO 测试粘性分区 改一下批次参数
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);  //已经过时了
        // TODO 修改分区策略
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);

        // 使用自定义的分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);

        //获取客户端连接对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //调用send()进行消息传送
        for (int i = 0; i < 10000; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("test", "niuniu-" + i), new Callback() {
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
            Thread.sleep(10);
        }

        //关闭资源
        kafkaProducer.close();
    }
}
