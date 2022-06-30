package com.adasarca.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class UserConsumer extends Thread {

    private final String serversConfig;
    private final String topic;

    public UserConsumer(String serversConfig, String topic) {
        this.serversConfig = serversConfig;
        this.topic = topic;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.serversConfig);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-test-consumer-group");

        try (KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonDeserializer<>(User.class))) {
            consumer.subscribe(Collections.singleton(this.topic));

            while (!this.isInterrupted()) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, User> record : records) {
                    User user = record.value();
                    System.out.printf("Received User record with id [%s], name[%s] and email [%s]%n",
                            user.getId(), user.getName(), user.getEmail());
                }
            }
        }
    }

}
