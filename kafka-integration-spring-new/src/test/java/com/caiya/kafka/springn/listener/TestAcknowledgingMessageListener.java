package com.caiya.kafka.springn.listener;

import com.caiya.kafka.springn.component.KafkaProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.Collections;

/**
 * TestAcknowledgingMessageListener.
 *
 * @author wangnan
 * @since 1.0.0, 2019/11/19
 **/
@Component
public class TestAcknowledgingMessageListener implements AcknowledgingMessageListener<String, String> {

    @Resource
    private KafkaProperties kafkaProperties;

    @Override
    public void onMessage(ConsumerRecords<String, String> data, Consumer<?, ?> consumer) {
        for (ConsumerRecord record : data) {
            logger().info("======record=====:" + record);
            consumer.commitSync();
        }
    }

    @Override
    public Collection<String> topics() {
        return Collections.singleton("test.auto");
    }

    @Override
    public String consumerFactoryName() {
        return "autoCommitConsumerFactory";
    }
}
