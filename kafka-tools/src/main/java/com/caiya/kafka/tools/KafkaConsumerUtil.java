package com.caiya.kafka.tools;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Kafka Consumer Util.
 *
 * @author wangnan
 * @since 1.2.0, 2020/5/18
 **/
public class KafkaConsumerUtil {


    /**
     * 消息回溯
     *
     * @param consumer         仅被初始化的消费者，没有订阅topic或拉去过消息
     * @param topic            消息topic
     * @param fromTimeInMillis 回溯开始时间，毫秒为单位
     * @param <K>              消息Key序列化器
     * @param <V>              消息Value序列化器
     * @return 是否成功
     */
    public static <K, V> boolean recall(KafkaConsumer<K, V> consumer, String topic, Long fromTimeInMillis) throws Exception {
        // firstly, subscribe and poll once
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(10);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        partitionInfos.forEach(partitionInfo -> topicPartitionLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fromTimeInMillis));

        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(topicPartitionLongMap);

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : topicPartitionOffsetAndTimestampMap.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            // may throw exception
            try {
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
            } catch (Exception e) {
                throw new Exception("Consumer Message recall failed, topic: " + topic + ", from: " + fromTimeInMillis, e);
            }
        }

        // remember to commit to the server-side broker
        consumer.commitSync();

        return true;
    }


}
