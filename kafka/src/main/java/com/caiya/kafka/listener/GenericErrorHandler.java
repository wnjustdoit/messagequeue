package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * A generic error handler.
 *
 * @param <T> the data type.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
@FunctionalInterface
public interface GenericErrorHandler<T> {

    /**
     * Handle the exception.
     * @param thrownException The exception.
     * @param data the data.
     */
    void handle(Exception thrownException, T data);

    /**
     * Handle the exception.
     * @param thrownException The exception.
     * @param data the data.
     * @param consumer the consumer.
     */
    default void handle(Exception thrownException, T data, Consumer<?, ?> consumer) {
        handle(thrownException, data);
    }

}
