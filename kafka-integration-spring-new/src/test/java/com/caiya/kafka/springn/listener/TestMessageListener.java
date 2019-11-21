package com.caiya.kafka.springn.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;

/**
 * TestMessageListener.
 * </p>
 * 自动提交测试
 *
 * @author wangnan
 * @since 1.0.0, 2019/11/19
 **/
@Component
public class TestMessageListener implements MessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecords<String, String> data) {
        for (ConsumerRecord record : data) {
            logger().info("======record=====:" + record);
        }
    }

    @Override
    public Collection<String> topics() {
        return Collections.singleton("test");
    }

}
