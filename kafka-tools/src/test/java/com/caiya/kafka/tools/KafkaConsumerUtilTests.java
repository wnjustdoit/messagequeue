package com.caiya.kafka.tools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * 消息回溯测试.
 *
 * @author wangnan
 * @since 1.2.0, 2020/5/15
 **/
public class KafkaConsumerUtilTests {

    private KafkaConsumer<String, String> consumer = null;

    @Before
    public void before() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9093,127.0.0.1:9094");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    @Test
    @Ignore
    public void test() {
        Long from = LocalDateTime.of(2020, 7, 7, 16, 18, 10).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        // LocalDateTime.of(2020, 5, 15, 13, 0).atZone(ZoneOffset.systemDefault()).toEpochSecond();
        boolean success;
        try {
            success = KafkaConsumerUtil.recall(consumer, "test", from);
        } catch (Exception e) {
            success = false;
            e.printStackTrace();
        }
        if (!success) {
            // maybe change the begin time and try again..
            // with spring retry
        }

        long start = System.currentTimeMillis();
        while (true) {
            // 设置1分钟结束
            if (System.currentTimeMillis() - start > 60000)
                break;

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    @After
    public void after() {
        consumer.close();
    }


}
