package com.caiya.kafka.spring.listener.adaptor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Implementations of this interface can signal that a record about
 * to be delivered to a message listener should be discarded instead
 * of being delivered.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public interface RecordFilterStrategy<K, V> {

    /**
     * Return true if the record should be discarded.
     * @param consumerRecord the record.
     * @return true to discard.
     */
    boolean filter(ConsumerRecord<K, V> consumerRecord);

}
