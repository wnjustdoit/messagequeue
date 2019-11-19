package com.caiya.kafka.springn;

import org.springframework.core.NestedRuntimeException;

/**
 * The Spring Kafka specific {@link NestedRuntimeException} implementation.
 */
public class KafkaException extends NestedRuntimeException {

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

}
