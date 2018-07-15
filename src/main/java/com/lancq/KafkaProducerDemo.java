package com.lancq;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author lancq
 * @Description
 * @Date 2018/7/14
 **/
public class KafkaProducerDemo extends Thread {

    private KafkaProducer<Integer, String> producer = null;
    private String topic = null;
    private boolean isAysnc = false;

    public KafkaProducerDemo(String topic, boolean isAysnc) {
        this.topic = topic;
        this.isAysnc = isAysnc;
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.227.129:9092,192.168.227.130:9092,192.168.227.131:9092");

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerDemo");

        //acks:
        //0：表示producer不需要等待broker的消息确认
        //1：表示producer只需要获得kafka集群中的leader节点确认即
        //all(-1)：需要ISR中所有的Replica给予接收确认，速度最慢，安全性最高，但是由于ISR可能会缩小到仅包含一个Replica，所以设置参数为all并不能一定避免数据丢失
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<Integer, String>(properties);
    }

    @Override
    public void run() {
        int num = 0;

        while(num < 50){
            String message = "message_" + num;
            System.out.println("begin send message:" + message);

            if(isAysnc){//异步发送
                Callback callback = new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(recordMetadata != null){
                            System.out.println("async-offset:" + recordMetadata.offset()+ "->partition" + recordMetadata.partition());

                        }
                    }
                };
                producer.send(new ProducerRecord<Integer, String>(topic,message),callback);

            } else { //同步发送

                try {
                    RecordMetadata recordMetadata = producer.send(new ProducerRecord<Integer, String>(topic,message)).get();

                    System.out.println("sync-offset:" + recordMetadata.offset()+ "->partition" + recordMetadata.partition());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            num ++;

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new KafkaProducerDemo("test",true).start();
    }
}
