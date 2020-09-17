package com.caiya.kafka.springn.listener;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Listener for handling incoming Kafka messages, propagating an acknowledgment handle that recipients
 * can invoke when the message has been processed.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author wangnan
 * @since 1.0.0, 2019/11/19
 */
public interface AcknowledgingMessageListener<K, V> extends com.caiya.kafka.springn.listener.GenericMessageListener<ConsumerRecords<K, V>> {

    @Override
    default void onMessage(ConsumerRecords<K, V> data) {
        throw new UnsupportedOperationException("This method should never be called");
    }

}
