package com.example.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMillis;

@Slf4j
public class ConsumerThread implements Runnable {
    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;
    private static final String bootstrapServer = "127.0.0.1:9092";
    private static final String groupId = "consumers_group_2";
    private static final String topic = "demo_java";
    private static final Properties properties = new Properties();

    public ConsumerThread(CountDownLatch latch) {
        this.latch = latch;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + "\n"
                            + "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "message: " + record);
                }
            }

        } catch (WakeupException e) {
            log.info("Recived shutdown signal ");
        }finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutDown() {
        //the wakeup method is a special method to interrup consumer.poll
        //it will throw WakeUpException
        consumer.wakeup();
    }
}
