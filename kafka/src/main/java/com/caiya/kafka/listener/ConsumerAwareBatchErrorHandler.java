package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * An error handler that has access to the consumer, for example to adjust
 * offsets after an error.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface ConsumerAwareBatchErrorHandler extends BatchErrorHandler {

    @Override
    default void handle(Exception thrownException, ConsumerRecords<?, ?> data) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer);

    @Override
    default void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
                        MessageListenerContainer container) {
        handle(thrownException, data, consumer);
    }

}
