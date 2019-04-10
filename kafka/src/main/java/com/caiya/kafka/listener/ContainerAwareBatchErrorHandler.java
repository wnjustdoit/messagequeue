package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * An error handler that has access to the batch of records from the last poll the
 * consumer, and the container.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
@FunctionalInterface
public interface ContainerAwareBatchErrorHandler extends ConsumerAwareBatchErrorHandler {

    @Override
    default void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
                MessageListenerContainer container);

}
