package com.caiya.kafka.spring.annotation;

import com.caiya.kafka.spring.config.KafkaListenerEndpointRegistry;
import com.caiya.kafka.spring.config.KafkaListenerConfigUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * {@code @Configuration} class that registers a {@link KafkaListenerAnnotationBeanPostProcessor}
 * bean capable of processing Spring's @{@link KafkaListener} annotation. Also register
 * a default {@link KafkaListenerEndpointRegistry}.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableKafka}
 * annotation.  See {@link EnableKafka} Javadoc for complete usage.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListenerEndpointRegistry
 * @see EnableKafka
 */
@Configuration
public class KafkaBootstrapConfiguration {

    @SuppressWarnings("rawtypes")
    @Bean(name = KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public KafkaListenerAnnotationBeanPostProcessor kafkaListenerAnnotationProcessor() {
        return new KafkaListenerAnnotationBeanPostProcessor();
    }

    @Bean(name = KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
    public KafkaListenerEndpointRegistry defaultKafkaListenerEndpointRegistry() {
        return new KafkaListenerEndpointRegistry();
    }

}
