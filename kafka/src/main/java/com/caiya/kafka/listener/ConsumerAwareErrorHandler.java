package com.caiya.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An error handler that has access to the consumer, for example to adjust
 * offsets after an error.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface ConsumerAwareErrorHandler extends ErrorHandler {

    @Override
    default void handle(Exception thrownException, ConsumerRecord<?, ?> data) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void handle(Exception thrownException, ConsumerRecord<?, ?> data, Consumer<?, ?> consumer);

    @Override
    default void handle(Exception thrownException, List<ConsumerRecord<?, ?>> data, Consumer<?, ?> consumer,
                        MessageListenerContainer container) {
        handle(thrownException, null, consumer);
    }

}
