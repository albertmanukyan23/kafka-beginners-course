package com.example.demo.producer;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThreads {


    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThread(latch);
        Thread thread = new Thread(myConsumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.info("Application has exited");
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.info("Application is interrupted", e);
        } finally {
            log.info("Application is closed");
        }
    }
}
