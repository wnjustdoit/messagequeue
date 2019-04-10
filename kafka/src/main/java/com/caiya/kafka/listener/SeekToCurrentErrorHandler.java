package com.caiya.kafka.listener;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.caiya.kafka.exception.KafkaException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * An error handler that seeks to the current offset for each topic in the remaining
 * records. Used to rewind partitions after a message failure so that it can be
 * replayed.
 *
 * @author Gary Russell
 * @since 2.0.1
 *
 */
public class SeekToCurrentErrorHandler implements ContainerAwareErrorHandler {

    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
                       Consumer<?, ?> consumer, MessageListenerContainer container) {
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
        records.forEach(r ->
                offsets.computeIfAbsent(new TopicPartition(r.topic(), r.partition()), k -> r.offset()));
        offsets.forEach(consumer::seek);
        throw new KafkaException("Seek to current after exception", thrownException);
    }

}
