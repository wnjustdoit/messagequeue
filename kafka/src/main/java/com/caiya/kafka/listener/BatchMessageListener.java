package com.caiya.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Listener for handling a batch of incoming Kafka messages; the list
 * is created from the consumer records object returned by a poll.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Marius Bogoevici
 * @author Gary Russell
 * @since 1.1
 */
@FunctionalInterface
public interface BatchMessageListener<K, V> extends GenericMessageListener<List<ConsumerRecord<K, V>>> {

}
