package com.caiya.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.messaging.Message;

/**
 * An error handler which is called when a {code @KafkaListener} method
 * throws an exception. This is invoked higher up the stack than the
 * listener container's error handler. For methods annotated with
 * {@code @SendTo}, the error handler can return a result.
 *
 * @author Venil Noronha
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 */
@FunctionalInterface
public interface KafkaListenerErrorHandler {

    /**
     * Handle the error.
     * @param message the spring-messaging message.
     * @param exception the exception the listener threw, wrapped in a
     * {@link ListenerExecutionFailedException}.
     * @return the return value is ignored unless the annotated method has a
     * {@code @SendTo} annotation.
     * @throws Exception an exception which may be the original or different.
     */
    Object handleError(Message<?> message, ListenerExecutionFailedException exception) throws Exception;

    /**
     * Handle the error.
     * @param message the spring-messaging message.
     * @param exception the exception the listener threw, wrapped in a
     * {@link ListenerExecutionFailedException}.
     * @param consumer the consumer.
     * @return the return value is ignored unless the annotated method has a
     * {@code @SendTo} annotation.
     * @throws Exception an exception which may be the original or different.
     */
    default Object handleError(Message<?> message, ListenerExecutionFailedException exception,
                               Consumer<?, ?> consumer) throws Exception {

        return handleError(message, exception);
    }

}
