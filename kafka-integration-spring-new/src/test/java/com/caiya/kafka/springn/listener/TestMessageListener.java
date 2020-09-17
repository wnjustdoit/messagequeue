package com.caiya.kafka.springn.listener;

import com.caiya.kafka.springn.component.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;

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

    @Resource
    private KafkaProperties kafkaProperties;

    @Override
    public void onMessage(ConsumerRecords<String, String> data) {
        for (ConsumerRecord<String, String> record : data) {
            logger().info("======record=====:" + record);
        }
    }

    @Override
    public Collection<String> topics() {
        return kafkaProperties.getTopics();
    }

}
