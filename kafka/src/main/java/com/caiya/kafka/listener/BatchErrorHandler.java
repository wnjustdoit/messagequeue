package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Handles errors thrown during the execution of a {@link BatchMessageListener}.
 * The listener should communicate which position(s) in the list failed in the
 * exception.
 *
 * @author Gary Russell
 *
 * @since 1.1
 */
public interface BatchErrorHandler extends GenericErrorHandler<ConsumerRecords<?, ?>> {

    /**
     * Handle the exception.
     * @param thrownException the exception.
     * @param data the consumer records.
     * @param consumer the consumer.
     * @param container the container.
     */
    default void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
                        MessageListenerContainer container) {
        handle(thrownException, data);
    }

}
