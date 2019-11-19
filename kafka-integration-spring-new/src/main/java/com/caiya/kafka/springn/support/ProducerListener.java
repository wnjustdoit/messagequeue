package com.caiya.kafka.springn.support;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Listener for handling outbound Kafka messages. Exactly one of its methods will be invoked, depending on whether
 * the write has been acknowledged or not.
 * <p>
 * Its main goal is to provide a stateless singleton delegate for {@link org.apache.kafka.clients.producer.Callback}s,
 * which, in all but the most trivial cases, requires creating a separate instance per message.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @see org.apache.kafka.clients.producer.Callback
 */
public interface ProducerListener<K, V> {

    /**
     * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker).
     *
     * @param producerRecord the actual sent record
     * @param recordMetadata the result of the successful send operation
     */
    default void onSuccess(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
        onSuccess(producerRecord.topic(), producerRecord.partition(),
                producerRecord.key(), producerRecord.value(), recordMetadata);
    }

    /**
     * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker).
     * If the method receiving the ProducerRecord is overridden, this method won't be called
     *
     * @param topic          the destination topic
     * @param partition      the destination partition (could be null)
     * @param key            the key of the outbound message
     * @param value          the payload of the outbound message
     * @param recordMetadata the result of the successful send operation
     */
    default void onSuccess(String topic, Integer partition, K key, V value, RecordMetadata recordMetadata) {
    }

    /**
     * Invoked after an attempt to send a message has failed.
     *
     * @param producerRecord the failed record
     * @param exception      the exception thrown
     */
    default void onError(ProducerRecord<K, V> producerRecord, Exception exception) {
        onError(producerRecord.topic(), producerRecord.partition(),
                producerRecord.key(), producerRecord.value(), exception);
    }

    /**
     * Invoked after an attempt to send a message has failed.
     * If the method receiving the ProducerRecord is overridden, this method won't be called
     *
     * @param topic     the destination topic
     * @param partition the destination partition (could be null)
     * @param key       the key of the outbound message
     * @param value     the payload of the outbound message
     * @param exception the exception thrown
     */
    default void onError(String topic, Integer partition, K key, V value, Exception exception) {
    }

    /**
     * Return true if this listener is interested in success as well as failure.
     *
     * @return true to express interest in successful sends.
     * @deprecated the result of this method will be ignored.
     */
    @Deprecated
    default boolean isInterestedInSuccess() {
        return false;
    }

}
