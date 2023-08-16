package com.example.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.time.Duration.*;

@Slf4j
public class ConsumerDemo {
    private static String bootstrapServer = "127.0.0.1:9092";
    private static String groupId = "consumers_group_1";
    private static String topic = "demo_java";
    private static Properties properties = new Properties();

    public static void main(String[] args) {


        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //subscribe consumer to our topics
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + "\n"
                        + "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "message: " + record);
            }
        }
    }

}
