package com.caiya.kafka.springn.listener;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Listener for handling individual incoming Kafka messages.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author wangnan
 * @since 1.0.0, 2019/11/19
 */
public interface MessageListener<K, V> extends GenericMessageListener<ConsumerRecords<K, V>> {

}
