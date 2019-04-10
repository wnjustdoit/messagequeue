package com.caiya.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Handles errors thrown during the execution of a {@link MessageListener}.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public interface ErrorHandler extends GenericErrorHandler<ConsumerRecord<?, ?>> {

    /**
     * Handle the exception.
     *
     * @param thrownException the exception.
     * @param records         the remaining records including the one that failed.
     * @param consumer        the consumer.
     * @param container       the container.
     */
    default void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
                        MessageListenerContainer container) {
        handle(thrownException, null);
    }

}
