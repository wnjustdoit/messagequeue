package com.caiya.kafka;

import org.apache.kafka.clients.producer.Producer;

/**
 * The strategy to produce a {@link Producer} instance(s).
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Gary Russell
 */
public interface ProducerFactory<K, V> {

    Producer<K, V> createProducer();

    default boolean transactionCapable() {
        return false;
    }
}
