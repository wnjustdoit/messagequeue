package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Listener for handling individual incoming Kafka messages.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
@FunctionalInterface
public interface MessageListener<K, V> extends GenericMessageListener<ConsumerRecord<K, V>> {

}
