package com.caiya.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An error handler that has access to the unprocessed records from the last poll
 * (including the failed record) and the consumer, for example to adjust offsets after an
 * error. The records passed to the handler will not be passed to the listener
 * (unless re-fetched if the handler performs seeks).
 *
 * @author Gary Russell
 * @since 2.0.1
 *
 */
@FunctionalInterface
public interface RemainingRecordsErrorHandler extends ConsumerAwareErrorHandler {

    @Override
    default void handle(Exception thrownException, ConsumerRecord<?, ?> data, Consumer<?, ?> consumer) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    /**
     * Handle the exception.
     * @param thrownException the exception.
     * @param records the remaining records including the one that failed.
     * @param consumer the consumer.
     */
    void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer);

    @Override
    default void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
                        MessageListenerContainer container) {
        handle(thrownException, records, consumer);
    }

}
