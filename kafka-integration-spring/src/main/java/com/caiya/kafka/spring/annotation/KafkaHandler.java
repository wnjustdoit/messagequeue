package com.caiya.kafka.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of a Kafka message
 * listener within a class that is annotated with {@link KafkaListener}.
 *
 * <p>See the {@link KafkaListener} for information about permitted method signatures
 * and available parameters.
 * <p><b>It is important to understand that when a message arrives, the method selection
 * depends on the payload type. The type is matched with a single non-annotated parameter,
 * or one that is annotated with {@code @Payload}.
 * There must be no ambiguity - the system
 * must be able to select exactly one method based on the payload type.</b>
 *
 * @author Gary Russell
 *
 * @see EnableKafka
 * @see KafkaListener
 * @see KafkaListenerAnnotationBeanPostProcessor
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface KafkaHandler {

    /**
     * When true, designate that this is the default fallback method if the payload type
     * matches no other {@link KafkaHandler} method. Only one method can be so designated.
     * @return true if this is the default method.
     * @since 2.1.3
     */
    boolean isDefault() default false;

}
