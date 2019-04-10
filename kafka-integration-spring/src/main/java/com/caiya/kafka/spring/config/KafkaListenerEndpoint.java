package com.caiya.kafka.spring.config;

import com.caiya.kafka.listener.MessageListenerContainer;
import com.caiya.kafka.spring.annotation.KafkaListenerConfigurer;
import com.caiya.kafka.support.TopicPartitionInitialOffset;
import com.caiya.kafka.support.converter.MessageConverter;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Model for a Kafka listener endpoint. Can be used against a
 * {@link KafkaListenerConfigurer
 * KafkaListenerConfigurer} to register endpoints programmatically.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public interface KafkaListenerEndpoint {

    /**
     * Return the id of this endpoint.
     * @return the id of this endpoint. The id can be further qualified
     * when the endpoint is resolved against its actual listener
     * container.
     * @see KafkaListenerContainerFactory#createListenerContainer
     */
    String getId();

    /**
     * Return the groupId of this endpoint - if present, overrides the
     * {@code group.id} property of the consumer factory.
     * @return the group id; may be null.
     * @since 1.3
     */
    String getGroupId();

    /**
     * Return the group of this endpoint or null if not in a group.
     * @return the group of this endpoint or null if not in a group.
     */
    String getGroup();

    /**
     * Return the topics for this endpoint.
     * @return the topics for this endpoint.
     */
    Collection<String> getTopics();

    /**
     * Return the topicPartitions for this endpoint.
     * @return the topicPartitions for this endpoint.
     */
    Collection<TopicPartitionInitialOffset> getTopicPartitions();

    /**
     * Return the topicPattern for this endpoint.
     * @return the topicPattern for this endpoint.
     */
    Pattern getTopicPattern();


    /**
     * Return the client id prefix for the container; it will be suffixed by
     * '-n' to provide a unique id when concurrency is used.
     * @return the client id prefix.
     * @since 2.1.1
     */
    String getClientIdPrefix();

    /**
     * Setup the specified message listener container with the model
     * defined by this endpoint.
     * <p>This endpoint must provide the requested missing option(s) of
     * the specified container to make it usable. Usually, this is about
     * setting the {@code queues} and the {@code messageListener} to
     * use but an implementation may override any default setting that
     * was already set.
     * @param listenerContainer the listener container to configure
     * @param messageConverter the message converter - can be null
     */
    void setupListenerContainer(MessageListenerContainer listenerContainer, MessageConverter messageConverter);

}
