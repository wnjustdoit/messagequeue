package com.caiya.kafka.spring.config;


import java.util.regex.Pattern;

import com.caiya.kafka.ConsumerFactory;
import com.caiya.kafka.KafkaTemplate;
import com.caiya.kafka.listener.*;
import com.caiya.kafka.listener.config.ContainerProperties;
import com.caiya.kafka.spring.listener.adaptor.RecordFilterStrategy;
import com.caiya.kafka.support.converter.MessageConverter;
import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 * Base {@link KafkaListenerContainerFactory} for Spring's base container implementation.
 *
 * @param <C> the {@link AbstractMessageListenerContainer} implementation type.
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @see AbstractMessageListenerContainer
 */
public abstract class AbstractKafkaListenerContainerFactory<C extends AbstractMessageListenerContainer<K, V>, K, V>
        implements KafkaListenerContainerFactory<C>, ApplicationEventPublisherAware {

    private final ContainerProperties containerProperties = new ContainerProperties((Pattern) null);

    private ConsumerFactory<K, V> consumerFactory;

    private Boolean autoStartup;

    private Integer phase;

    private MessageConverter messageConverter;

    private RecordFilterStrategy<K, V> recordFilterStrategy;

    private Boolean ackDiscarded;

    private RetryTemplate retryTemplate;

    private RecoveryCallback<? extends Object> recoveryCallback;

    private Boolean statefulRetry;

    private Boolean batchListener;

    private ApplicationEventPublisher applicationEventPublisher;

    private KafkaTemplate<K, V> replyTemplate;

    private AfterRollbackProcessor<K, V> afterRollbackProcessor;

    /**
     * Specify a {@link ConsumerFactory} to use.
     * @param consumerFactory The consumer factory.
     */
    public void setConsumerFactory(ConsumerFactory<K, V> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public ConsumerFactory<K, V> getConsumerFactory() {
        return this.consumerFactory;
    }

    /**
     * Specify an {@code autoStartup boolean} flag.
     * @param autoStartup true for auto startup.
     * @see AbstractMessageListenerContainer#setAutoStartup(boolean)
     */
    public void setAutoStartup(Boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    /**
     * Specify a {@code phase} to use.
     * @param phase The phase.
     * @see AbstractMessageListenerContainer#setPhase(int)
     */
    public void setPhase(int phase) {
        this.phase = phase;
    }

    /**
     * Set the message converter to use if dynamic argument type matching is needed.
     * @param messageConverter the converter.
     */
    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    /**
     * Set the record filter strategy.
     * @param recordFilterStrategy the strategy.
     */
    public void setRecordFilterStrategy(RecordFilterStrategy<K, V> recordFilterStrategy) {
        this.recordFilterStrategy = recordFilterStrategy;
    }

    /**
     * Set to true to ack discards when a filter strategy is in use.
     * @param ackDiscarded the ackDiscarded.
     */
    public void setAckDiscarded(Boolean ackDiscarded) {
        this.ackDiscarded = ackDiscarded;
    }

    /**
     * Set a retryTemplate.
     * @param retryTemplate the template.
     */
    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    /**
     * Set a callback to be used with the {@link #setRetryTemplate(RetryTemplate)
     * retryTemplate}.
     * @param recoveryCallback the callback.
     */
    public void setRecoveryCallback(RecoveryCallback<? extends Object> recoveryCallback) {
        this.recoveryCallback = recoveryCallback;
    }

    /**
     * When using a {@link RetryTemplate} Set to true to enable stateful retry. Use in
     * conjunction with a
     * {@link SeekToCurrentErrorHandler} when retry can
     * take excessive time; each failure goes back to the broker, to keep the Consumer
     * alive.
     * @param statefulRetry true to enable stateful retry.
     * @since 2.1.3
     */
    public void setStatefulRetry(boolean statefulRetry) {
        this.statefulRetry = statefulRetry;
    }


    /**
     * Return true if this endpoint creates a batch listener.
     * @return true for a batch listener.
     * @since 1.1
     */
    public Boolean isBatchListener() {
        return this.batchListener;
    }

    /**
     * Set to true if this endpoint should create a batch listener.
     * @param batchListener true for a batch listener.
     * @since 1.1
     */
    public void setBatchListener(Boolean batchListener) {
        this.batchListener = batchListener;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Set the {@link KafkaTemplate} to use to send replies.
     * @param replyTemplate the template.
     * @since 2.0
     */
    public void setReplyTemplate(KafkaTemplate<K, V> replyTemplate) {
        this.replyTemplate = replyTemplate;
    }

    /**
     * Set a processor to invoke after a transaction rollback; typically will
     * seek the unprocessed topic/partition to reprocess the records.
     * The default does so, including the failed record.
     * @param afterRollbackProcessor the processor.
     * @since 1.3.5
     */
    public void setAfterRollbackProcessor(AfterRollbackProcessor<K, V> afterRollbackProcessor) {
        this.afterRollbackProcessor = afterRollbackProcessor;
    }

    /**
     * Obtain the properties template for this factory - set properties as needed
     * and they will be copied to a final properties instance for the endpoint.
     * @return the properties.
     */
    public ContainerProperties getContainerProperties() {
        return this.containerProperties;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C createListenerContainer(KafkaListenerEndpoint endpoint) {
        C instance = createContainerInstance(endpoint);

        if (this.autoStartup != null) {
            instance.setAutoStartup(this.autoStartup);
        }
        if (this.phase != null) {
            instance.setPhase(this.phase);
        }
        if (this.applicationEventPublisher != null) {
           // instance.setApplicationEventPublisher(this.applicationEventPublisher);
        }
        if (endpoint.getId() != null) {
            instance.setBeanName(endpoint.getId());
        }

        if (endpoint instanceof AbstractKafkaListenerEndpoint) {
            AbstractKafkaListenerEndpoint<K, V> aklEndpoint = (AbstractKafkaListenerEndpoint<K, V>) endpoint;
            if (this.recordFilterStrategy != null) {
                aklEndpoint.setRecordFilterStrategy(this.recordFilterStrategy);
            }
            if (this.ackDiscarded != null) {
                aklEndpoint.setAckDiscarded(this.ackDiscarded);
            }
            if (this.retryTemplate != null) {
                aklEndpoint.setRetryTemplate(this.retryTemplate);
            }
            if (this.recoveryCallback != null) {
                aklEndpoint.setRecoveryCallback(this.recoveryCallback);
            }
            if (this.statefulRetry != null) {
                aklEndpoint.setStatefulRetry(this.statefulRetry);
            }
            if (this.batchListener != null) {
                aklEndpoint.setBatchListener(this.batchListener);
            }
            if (this.replyTemplate != null) {
                aklEndpoint.setReplyTemplate(this.replyTemplate);
            }
        }

        endpoint.setupListenerContainer(instance, this.messageConverter);
        initializeContainer(instance);
        instance.getContainerProperties().setGroupId(endpoint.getGroupId());
        instance.getContainerProperties().setClientId(endpoint.getClientIdPrefix());

        return instance;
    }

    /**
     * Create an empty container instance.
     * @param endpoint the endpoint.
     * @return the new container instance.
     */
    protected abstract C createContainerInstance(KafkaListenerEndpoint endpoint);

    /**
     * Further initialize the specified container.
     * <p>Subclasses can inherit from this method to apply extra
     * configuration if necessary.
     * @param instance the container instance to configure.
     */
    protected void initializeContainer(C instance) {
        ContainerProperties properties = instance.getContainerProperties();
        BeanUtils.copyProperties(this.containerProperties, properties, "topics", "topicPartitions", "topicPattern",
                "messageListener", "ackCount", "ackTime");
        if (this.afterRollbackProcessor != null) {
            instance.setAfterRollbackProcessor(this.afterRollbackProcessor);
        }
        if (this.containerProperties.getAckCount() > 0) {
            properties.setAckCount(this.containerProperties.getAckCount());
        }
        if (this.containerProperties.getAckTime() > 0) {
            properties.setAckTime(this.containerProperties.getAckTime());
        }
        if (this.containerProperties.getGenericErrorHandler() instanceof BatchErrorHandler) {
            properties.setBatchErrorHandler((BatchErrorHandler) this.containerProperties.getGenericErrorHandler());
        }
        else {
            properties.setErrorHandler((ErrorHandler) this.containerProperties.getGenericErrorHandler());
        }
    }

}
