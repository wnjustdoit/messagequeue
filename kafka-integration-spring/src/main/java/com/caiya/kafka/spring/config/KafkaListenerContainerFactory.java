package com.caiya.kafka.spring.config;

import com.caiya.kafka.listener.MessageListenerContainer;

/**
 * Factory of {@link MessageListenerContainer} based on a
 * {@link KafkaListenerEndpoint} definition.
 *
 * @param <C> the {@link MessageListenerContainer} implementation type.
 *
 * @author Stephane Nicoll
 *
 * @see KafkaListenerEndpoint
 */
public interface KafkaListenerContainerFactory<C extends MessageListenerContainer> {

    /**
     * Create a {@link MessageListenerContainer} for the given {@link KafkaListenerEndpoint}.
     * @param endpoint the endpoint to configure
     * @return the created container
     */
    C createListenerContainer(KafkaListenerEndpoint endpoint);

}
