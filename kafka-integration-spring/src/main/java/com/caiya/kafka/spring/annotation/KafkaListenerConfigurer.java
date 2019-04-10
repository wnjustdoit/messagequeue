package com.caiya.kafka.spring.annotation;

import com.caiya.kafka.spring.config.KafkaListenerContainerFactory;
import com.caiya.kafka.spring.config.KafkaListenerEndpoint;
import com.caiya.kafka.spring.config.KafkaListenerEndpointRegistrar;
import com.caiya.kafka.spring.config.KafkaListenerEndpointRegistry;

/**
 * Optional interface to be implemented by Spring managed bean willing
 * to customize how Kafka listener endpoints are configured. Typically
 * used to defined the default
 * {@link KafkaListenerContainerFactory
 * KafkaListenerContainerFactory} to use or for registering Kafka endpoints
 * in a <em>programmatic</em> fashion as opposed to the <em>declarative</em>
 * approach of using the @{@link KafkaListener} annotation.
 *
 * <p>See @{@link EnableKafka} for detailed usage examples.
 *
 * @author Stephane Nicoll
 *
 * @see EnableKafka
 * @see KafkaListenerEndpointRegistrar
 */
public interface KafkaListenerConfigurer {

    /**
     * Callback allowing a {@link KafkaListenerEndpointRegistry
     * KafkaListenerEndpointRegistry} and specific {@link KafkaListenerEndpoint
     * KafkaListenerEndpoint} instances to be registered against the given
     * {@link KafkaListenerEndpointRegistrar}. The default
     * {@link KafkaListenerContainerFactory KafkaListenerContainerFactory}
     * can also be customized.
     * @param registrar the registrar to be configured
     */
    void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar);

}
