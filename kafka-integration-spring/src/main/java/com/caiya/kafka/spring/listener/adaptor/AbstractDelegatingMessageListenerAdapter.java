package com.caiya.kafka.spring.listener.adaptor;

import java.util.Map;

import com.caiya.kafka.listener.ConsumerSeekAware;
import com.caiya.kafka.listener.DelegatingMessageListener;
import com.caiya.kafka.listener.ListenerType;
import com.caiya.kafka.listener.ListenerUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

/**
 * Top level class for all listener adapters.
 *
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public abstract class AbstractDelegatingMessageListenerAdapter<T>
        implements ConsumerSeekAware, DelegatingMessageListener<T> {

    protected final Log logger = LogFactory.getLog(this.getClass()); // NOSONAR

    protected final T delegate; //NOSONAR

    protected final ListenerType delegateType;

    private final ConsumerSeekAware seekAware;

    public AbstractDelegatingMessageListenerAdapter(T delegate) {
        this.delegate = delegate;
        this.delegateType = ListenerUtils.determineListenerType(delegate);
        if (delegate instanceof ConsumerSeekAware) {
            this.seekAware = (ConsumerSeekAware) delegate;
        }
        else {
            this.seekAware = null;
        }
    }

    @Override
    public T getDelegate() {
        return this.delegate;
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        if (this.seekAware != null) {
            this.seekAware.registerSeekCallback(callback);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (this.seekAware != null) {
            this.seekAware.onPartitionsAssigned(assignments, callback);
        }
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (this.seekAware != null) {
            this.seekAware.onIdleContainer(assignments, callback);
        }
    }

}
