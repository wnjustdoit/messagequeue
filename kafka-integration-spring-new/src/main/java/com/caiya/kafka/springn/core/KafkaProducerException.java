package com.caiya.kafka.springn.core;

import com.caiya.kafka.springn.KafkaException;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Exceptions when producing.
 */
@SuppressWarnings("serial")
public class KafkaProducerException extends KafkaException {

    private final ProducerRecord<?, ?> producerRecord;

    public KafkaProducerException(ProducerRecord<?, ?> failedProducerRecord, String message, Throwable cause) {
        super(message, cause);
        this.producerRecord = failedProducerRecord;
    }

    public ProducerRecord<?, ?> getProducerRecord() {
        return this.producerRecord;
    }

}
