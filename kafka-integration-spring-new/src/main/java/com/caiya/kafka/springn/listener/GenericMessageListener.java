package com.caiya.kafka.springn.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Top level interface for listeners.
 *
 * @param <T> the type received by the listener.
 * @author wangnan
 * @since 1.0.0, 2019/11/19
 **/
public interface GenericMessageListener<T> {

    /**
     * Default consumer's polling timeout in milliseconds.
     */
    long POLL_TIMEOUT_IN_MILLIS = 100;

    /**
     * @return logger instance of subclasses.
     */
    default Logger logger() {
        return LoggerFactory.getLogger(this.getClass());
    }

    /**
     * @return topics to subscribe.
     */
    Collection<String> topics();

    /**
     * Use default autowired instance if null.
     *
     * @return consumer factory bean name.
     */
    default String consumerFactoryName() {
        return null;
    }

    /**
     * If null, subscribe all partitions.
     *
     * @return partitions to subscribe.
     */
    default Collection<Integer> partitions() {
        return null;
    }

    /**
     * @return polling timeout in millis.
     */
    default long pollTimeoutInMillis() {
        return POLL_TIMEOUT_IN_MILLIS;
    }

    /**
     * Invoked with data from kafka.
     *
     * @param data the data to be processed.
     */
    void onMessage(T data);

    /**
     * Invoked with data from kafka and provides access to the {@link Consumer}
     * for operations such as pause/resume. Invoked with null data when a poll
     * returns no data (enabling resume). The default implementation throws
     * {@link UnsupportedOperationException}.
     *
     * @param data     the data to be processed.
     * @param consumer the consumer.
     */
    default void onMessage(T data, Consumer<?, ?> consumer) {
        throw new UnsupportedOperationException("This method should never be called");
    }

}
