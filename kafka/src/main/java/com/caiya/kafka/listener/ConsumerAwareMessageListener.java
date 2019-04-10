package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Listener for handling individual incoming Kafka messages.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 2.0
 */
@FunctionalInterface
public interface ConsumerAwareMessageListener<K, V> extends MessageListener<K, V> {

    /**
     * Invoked with data from kafka. Containers should never call this since it they
     * will detect we are a consumer aware acknowledging listener.
     * @param data the data to be processed.
     */
    @Override
    default void onMessage(ConsumerRecord<K, V> data) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void onMessage(ConsumerRecord<K, V> data, Consumer<?, ?> consumer);

}
