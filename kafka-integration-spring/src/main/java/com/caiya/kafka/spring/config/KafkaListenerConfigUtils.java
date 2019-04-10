package com.caiya.kafka.spring.config;

/**
 * Configuration constants for internal sharing across subpackages.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 */
public abstract class KafkaListenerConfigUtils {

    /**
     * The bean name of the internally managed Kafka listener annotation processor.
     */
    public static final String KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
            "org.springframework.kafka.config.internalKafkaListenerAnnotationProcessor";

    /**
     * The bean name of the internally managed Kafka listener endpoint registry.
     */
    public static final String KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
            "org.springframework.kafka.config.internalKafkaListenerEndpointRegistry";

}
