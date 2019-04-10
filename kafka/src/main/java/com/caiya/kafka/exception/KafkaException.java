package com.caiya.kafka.exception;

/**
 * The Spring Kafka specific {@link NestedRuntimeException} implementation.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
@SuppressWarnings("serial")
public class KafkaException extends NestedRuntimeException {

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

}
