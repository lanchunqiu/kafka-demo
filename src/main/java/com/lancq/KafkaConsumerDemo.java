package com.lancq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author lancq
 * @Description
 * @Date 2018/7/14
 **/
public class KafkaConsumerDemo extends Thread {
    Logger Log = LoggerFactory.getLogger(KafkaConsumerDemo.class);
    private KafkaConsumer<Integer,String> kafkaConsumer;

    public KafkaConsumerDemo(String topic){
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.227.129:9092,192.168.227.130:9092,192.168.227.131:9092");

        //不配置groupId，会抛异常
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerDemo");

        //消费者消费消息以后自动提交，只有当消息提交以后，该消息才不会被再次接收到，还可以配合auto.commit.interval.ms控制自动提交的频
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //latest情况下，新的消费者将会从其他消费者最后消费的offset处开始消费Topic下的消息
        //earliest情况下，新的消费者会从该topic最早的消息开始消费
        //none情况下，新的消费者加入以后，由于之前不存在offset，则会直接抛出异常
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        System.out.println("properties = [" + properties + "]");

        kafkaConsumer = new KafkaConsumer<Integer,String>(properties);

        kafkaConsumer.subscribe(Collections.singletonList(topic));

    }

    @Override
    public void run(){
        while(true){
            ConsumerRecords<Integer,String> consumerRecord = kafkaConsumer.poll(1000);
            for(ConsumerRecord record : consumerRecord){
                System.out.println("message receive:" + record.value());
                //提交
                kafkaConsumer.commitAsync();
            }
        }

    }

    public static void main(String[] args) {
        new KafkaConsumerDemo("test").start();
    }
}
