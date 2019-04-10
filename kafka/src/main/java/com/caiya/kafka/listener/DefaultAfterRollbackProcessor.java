package com.caiya.kafka.listener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link AfterRollbackProcessor}. Seeks all
 * topic/partitions so the records will be re-fetched, including the failed
 * record.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
public class DefaultAfterRollbackProcessor<K, V> implements AfterRollbackProcessor<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAfterRollbackProcessor.class);

    @Override
    public void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer) {
        Map<TopicPartition, Long> partitions = new HashMap<>();
        records.forEach(r -> partitions.computeIfAbsent(new TopicPartition(r.topic(), r.partition()),
                offset -> r.offset()));
        partitions.forEach((topicPartition, offset) -> {
            try {
                consumer.seek(topicPartition, offset);
            }
            catch (Exception e) {
                logger.error("Failed to seek " + topicPartition + " to " + offset);
            }
        });
    }

}
