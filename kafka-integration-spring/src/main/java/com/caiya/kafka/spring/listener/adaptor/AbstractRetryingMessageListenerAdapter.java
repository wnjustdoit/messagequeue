package com.caiya.kafka.spring.listener.adaptor;

import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base class for retrying message listener adapters.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 *
 */
public abstract class AbstractRetryingMessageListenerAdapter<K, V, T>
        extends AbstractDelegatingMessageListenerAdapter<T> {

    private final RetryTemplate retryTemplate;

    private final RecoveryCallback<? extends Object> recoveryCallback;

    /**
     * Construct an instance with the supplied retry template. The exception will be
     * thrown to the container after retries are exhausted.
     * @param delegate the delegate listener.
     * @param retryTemplate the template.
     */
    public AbstractRetryingMessageListenerAdapter(T delegate, RetryTemplate retryTemplate) {
        this(delegate, retryTemplate, null);
    }

    /**
     * Construct an instance with the supplied template and callback.
     * @param delegate the delegate listener.
     * @param retryTemplate the template.
     * @param recoveryCallback the recovery callback; if null, the exception will be
     * thrown to the container after retries are exhausted.
     */
    public AbstractRetryingMessageListenerAdapter(T delegate, RetryTemplate retryTemplate,
                                                  RecoveryCallback<? extends Object> recoveryCallback) {
        super(delegate);
        Assert.notNull(retryTemplate, "'retryTemplate' cannot be null");
        this.retryTemplate = retryTemplate;
        this.recoveryCallback = recoveryCallback;
    }

    public RetryTemplate getRetryTemplate() {
        return this.retryTemplate;
    }

    public RecoveryCallback<? extends Object> getRecoveryCallback() {
        return this.recoveryCallback;
    }

}
