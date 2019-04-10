package com.caiya.kafka.spring.test;

import com.caiya.kafka.KafkaTemplate;
import com.caiya.kafka.spring.annotation.KafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 应用启动入口.
 *
 * @author wangnan
 * @since 1.0
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.caiya")
public class Application implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    private final CountDownLatch latch = new CountDownLatch(40);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    @KafkaListener(topics = "mall", groupId = "test")
    public void listener(ConsumerRecord<String, String> message) {
        logger.info("received: " + message);
        latch.countDown();
    }

    @Override
    public void run(String... args) throws Exception {
        kafkaTemplate.sendDefault("0", "foo");
        kafkaTemplate.sendDefault("2", "bar");
        kafkaTemplate.sendDefault("0", "baz");
        kafkaTemplate.sendDefault("2", "qux");
        latch.await(10, TimeUnit.SECONDS);
        logger.info("All received");
    }
}
