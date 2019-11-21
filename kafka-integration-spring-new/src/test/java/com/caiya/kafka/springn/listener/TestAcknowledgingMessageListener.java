package com.caiya.kafka.springn.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * TestAcknowledgingMessageListener.
 * </p>
 * 手动提交测试。
 * </p>
 * 可以暂存{@link ConsumerRecord}到内存中，直至数量超过指定的值或者超过指定的时间必须提交（批量）；
 * 可以通过分区遍历进行消费（如果分区订阅，必须分区手动提交）。
 *
 * @author wangnan
 * @see <a href="http://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html">KafkaConsumer</a>
 * @since 1.0.0, 2019/11/19
 **/
@Component
public class TestAcknowledgingMessageListener implements AcknowledgingMessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecords<String, String> data, Consumer<?, ?> consumer) {
        // 消费方式一：
        for (ConsumerRecord record : data) {
            logger().info("======record=====:" + record);
            consumer.commitSync();
        }

        // 消费方式二：
//        for (TopicPartition partition : data.partitions()) {
//            List<ConsumerRecord<String, String>> partitionRecords = data.records(partition);
//            for (ConsumerRecord<String, String> record : partitionRecords) {
//                System.out.println(record.offset() + ": " + record.value());
//            }
//            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//        }
    }

    @Override
    public Collection<String> topics() {
        return Collections.singleton("test.manual");
    }

    @Override
    public String consumerFactoryName() {
        return "manualCommitConsumerFactory";
    }

    @Override
    public Collection<Integer> partitions() {
        return Arrays.asList(0);
    }

}
