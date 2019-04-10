package com.caiya.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * The strategy to produce a {@link Consumer} instance(s).
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Gary Russell
 */
public interface ConsumerFactory<K, V> {

    /**
     * Create a consumer with the group id and client id as configured in the properties.
     *
     * @return the consumer.
     */
    Consumer<K, V> createConsumer();

    /**
     * Create a consumer, appending the suffix to the {@code client.id} property,
     * if present.
     *
     * @param clientIdSuffix the suffix.
     * @return the consumer.
     * @since 1.3
     */
    Consumer<K, V> createConsumer(String clientIdSuffix);

    /**
     * Create a consumer with an explicit group id; in addition, the
     * client id suffix is appended to the {@code client.id} property, if both
     * are present.
     *
     * @param groupId        the group id.
     * @param clientIdSuffix the suffix.
     * @return the consumer.
     * @since 1.3
     */
    Consumer<K, V> createConsumer(String groupId, String clientIdSuffix);

    /**
     * Create a consumer with an explicit group id; in addition, the
     * client id suffix is appended to the clientIdPrefix which overrides the
     * {@code client.id} property, if present.
     * If a factory does not implement this method, {@link #createConsumer(String, String)}
     * is invoked, ignoring the prefix.
     * TODO: remove default in 2.2
     *
     * @param groupId        the group id.
     * @param clientIdPrefix the prefix.
     * @param clientIdSuffix the suffix.
     * @return the consumer.
     * @since 2.1.1
     */
    default Consumer<K, V> createConsumer(String groupId, String clientIdPrefix, String clientIdSuffix) {
        return createConsumer(groupId, clientIdSuffix);
    }

    /**
     * Return true if consumers created by this factory use auto commit.
     *
     * @return true if auto commit.
     */
    boolean isAutoCommit();

    /**
     * Return an unmodifiable reference to the configuration map for this factory.
     * Useful for cloning to make a similar factory.
     *
     * @return the configs.
     * @since 2.0
     */
    default Map<String, Object> getConfigurationProperties() {
        throw new UnsupportedOperationException("'getConfigurationProperties()' is not supported");
    }

    /**
     * Return the configured key deserializer (if provided as an object instead
     * of a class name in the properties).
     *
     * @return the deserializer.
     * @since 2.0
     */
    default Deserializer<K> getKeyDeserializer() {
        throw new UnsupportedOperationException("'getKeyDeserializer()' is not supported");
    }

    /**
     * Return the configured value deserializer (if provided as an object instead
     * of a class name in the properties).
     *
     * @return the deserializer.
     * @since 2.0
     */
    default Deserializer<V> getValueDeserializer() {
        throw new UnsupportedOperationException("'getKeyDeserializer()' is not supported");
    }

}
