package com.adasarca.kafkademo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class UserProducer extends Thread {

    private final String serversConfig;
    private final String topic;
    private final int users;

    public UserProducer(String serversConfig, String topic, int users) {
        this.serversConfig = serversConfig;
        this.topic = topic;
        this.users = users;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.serversConfig);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        try (KafkaProducer<String, User> producer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= this.users; i++) {
                User user = new User();
                user.setId(UUID.randomUUID().toString());
                user.setName("Name-" + i);
                user.setEmail(user.getName() + "@test.com");

                ProducerRecord<String, User> record = new ProducerRecord<>(topic, user);
                producer.send(record, (recordMetadata, e) -> {
                    if (null == e) {
                        System.out.printf("Successfully published user record to topic [%s], partition [%d], offset[%d], timestamp [%d]%n",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        System.out.println("Exception publishing user record: " + e);
                    }
                });

                Thread.sleep(2000);
            }

            producer.flush();
        } catch (Exception exception) {
            System.out.println("Exception running producer: " + exception);
        }
    }
}
