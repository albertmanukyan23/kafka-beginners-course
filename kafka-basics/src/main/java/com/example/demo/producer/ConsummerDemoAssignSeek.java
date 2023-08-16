package com.example.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.time.Duration.ofMillis;

@Slf4j
public class ConsummerDemoAssignSeek {
    private static String bootstrapServer = "127.0.0.1:9092";

    private static String topic = "demo_java";
    private static Properties properties = new Properties();

    public static void main(String[] args) {


        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //assign and seek are mostly used to replay data or fetch a specific message
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        int numberOfMessagesToRead = 1;
        boolean keepOnReading = true;
        int numberOfMessagesSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesSoFar += 1;
                log.info("Key: " + record.key() + "\n"
                        + "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "message: " + record);
                if (numberOfMessagesSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }

}
