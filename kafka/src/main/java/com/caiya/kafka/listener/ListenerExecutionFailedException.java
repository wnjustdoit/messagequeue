package com.caiya.kafka.listener;

import com.caiya.kafka.exception.KafkaException;

/**
 * The listener specific {@link KafkaException} extension.
 *
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class ListenerExecutionFailedException extends KafkaException {

    public ListenerExecutionFailedException(String message) {
        super(message);
    }

    public ListenerExecutionFailedException(String message, Throwable cause) {
        super(message, cause);
    }

}
