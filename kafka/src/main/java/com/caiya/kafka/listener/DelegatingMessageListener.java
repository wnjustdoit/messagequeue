package com.caiya.kafka.listener;

/**
 * Classes implementing this interface allow containers to determine the type of the
 * ultimate listener.
 *
 * @param <T> the type received by the listener.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public interface DelegatingMessageListener<T> {

    /**
     * Return the delegate.
     * @return the delegate.
     */
    T getDelegate();

}
