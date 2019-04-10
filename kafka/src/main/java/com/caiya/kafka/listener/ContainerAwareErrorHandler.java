package com.caiya.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An error handler that has access to the unprocessed records from the last poll
 * (including the failed record), the consumer, and the container.
 * The records passed to the handler will not be passed to the listener
 * (unless re-fetched if the handler performs seeks).
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
@FunctionalInterface
public interface ContainerAwareErrorHandler extends RemainingRecordsErrorHandler {

    @Override
    default void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
                MessageListenerContainer container);

}
