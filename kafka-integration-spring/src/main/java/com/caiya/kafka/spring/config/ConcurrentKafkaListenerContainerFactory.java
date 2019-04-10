package com.caiya.kafka.spring.config;

import java.util.Collection;

import com.caiya.kafka.listener.ConcurrentMessageListenerContainer;
import com.caiya.kafka.listener.config.ContainerProperties;
import com.caiya.kafka.support.TopicPartitionInitialOffset;

/**
 * A {@link KafkaListenerContainerFactory} implementation to build a
 * {@link ConcurrentMessageListenerContainer}.
 * <p>
 * This should be the default for most users and a good transition paths
 * for those that are used to build such container definitions manually.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Murali Reddy
 */
public class ConcurrentKafkaListenerContainerFactory<K, V>
        extends AbstractKafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>, K, V> {

    private Integer concurrency;

    /**
     * Specify the container concurrency.
     * @param concurrency the number of consumers to create.
     * @see ConcurrentMessageListenerContainer#setConcurrency(int)
     */
    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    @Override
    protected ConcurrentMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {
        Collection<TopicPartitionInitialOffset> topicPartitions = endpoint.getTopicPartitions();
        if (!topicPartitions.isEmpty()) {
            ContainerProperties properties = new ContainerProperties(
                    topicPartitions.toArray(new TopicPartitionInitialOffset[topicPartitions.size()]));
            return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
        }
        else {
            Collection<String> topics = endpoint.getTopics();
            if (!topics.isEmpty()) {
                ContainerProperties properties = new ContainerProperties(topics.toArray(new String[topics.size()]));
                return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
            }
            else {
                ContainerProperties properties = new ContainerProperties(endpoint.getTopicPattern());
                return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
            }
        }
    }

    @Override
    protected void initializeContainer(ConcurrentMessageListenerContainer<K, V> instance) {
        super.initializeContainer(instance);
        if (this.concurrency != null) {
            instance.setConcurrency(this.concurrency);
        }
    }

}
