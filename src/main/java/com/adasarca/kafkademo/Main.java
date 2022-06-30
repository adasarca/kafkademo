package com.adasarca.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        UserProducer userProducer = new UserProducer("127.0.0.1:9092", "UserTopic", 3);
        userProducer.start();

        UserConsumer userConsumer = new UserConsumer("127.0.0.1:9092", "UserTopic");
        userConsumer.start();
    }
}
