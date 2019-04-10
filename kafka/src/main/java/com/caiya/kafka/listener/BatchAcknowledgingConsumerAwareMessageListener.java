package com.caiya.kafka.listener;

import java.util.List;

import com.caiya.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Listener for handling a batch of incoming Kafka messages, propagating an acknowledgment
 * handle that recipients can invoke when the message has been processed. The list is
 * created from the consumer records object returned by a poll. Access to the
 * {@link Consumer} is provided.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 2.0
 */
@FunctionalInterface
public interface BatchAcknowledgingConsumerAwareMessageListener<K, V> extends BatchMessageListener<K, V> {

    /**
     * Invoked with data from kafka. Containers should never call this since it they
     * will detect that we are a consumer aware acknowledging listener.
     * @param data the data to be processed.
     */
    @Override
    default void onMessage(List<ConsumerRecord<K, V>> data) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void onMessage(List<ConsumerRecord<K, V>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer);

}
