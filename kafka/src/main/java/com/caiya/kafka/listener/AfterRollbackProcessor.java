package com.caiya.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Invoked by a listener container with remaining, unprocessed, records
 * (including the failed record). Implementations should seek the desired
 * topics/partitions so that records will be re-fetched on the next
 * poll. When used with a batch listener, the entire batch of records is
 * provided.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
@FunctionalInterface
public interface AfterRollbackProcessor<K, V> {

    /**
     * Process the remaining records.
     * @param records the records.
     * @param consumer the consumer.
     */
    void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer);

}
