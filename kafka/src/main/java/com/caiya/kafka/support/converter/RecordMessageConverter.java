package com.caiya.kafka.support.converter;

import java.lang.reflect.Type;

import com.caiya.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.messaging.Message;

/**
 * A Kafka-specific {@link Message} converter strategy.
 *
 * @author Gary Russell
 * @since 1.1
 */
public interface RecordMessageConverter extends MessageConverter {

    /**
     * Convert a {@link ConsumerRecord} to a {@link Message}.
     *
     * @param record         the record.
     * @param acknowledgment the acknowledgment.
     * @param consumer       the consumer
     * @param payloadType    the required payload type.
     * @return the message.
     */
    Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer,
                         Type payloadType);

    /**
     * Convert a message to a producer record.
     *
     * @param message      the message.
     * @param defaultTopic the default topic to use if no header found.
     * @return the producer record.
     */
    ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic);

}
