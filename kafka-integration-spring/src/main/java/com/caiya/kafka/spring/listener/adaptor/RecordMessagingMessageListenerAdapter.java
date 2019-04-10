package com.caiya.kafka.spring.listener.adaptor;

import java.lang.reflect.Method;

import com.caiya.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import com.caiya.kafka.listener.KafkaListenerErrorHandler;
import com.caiya.kafka.listener.ListenerExecutionFailedException;
import com.caiya.kafka.listener.MessageListener;
import com.caiya.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.messaging.Message;


/**
 * A {@link MessageListener MessageListener}
 * adapter that invokes a configurable {@link HandlerAdapter}; used when the factory is
 * configured for the listener to receive individual messages.
 *
 * <p>Wraps the incoming Kafka Message to Spring's {@link Message} abstraction.
 *
 * <p>The original {@link ConsumerRecord} and
 * the {@link Acknowledgment} are provided as additional arguments so that these can
 * be injected as method arguments if necessary.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Venil Noronha
 */
public class RecordMessagingMessageListenerAdapter<K, V> extends MessagingMessageListenerAdapter<K, V>
        implements AcknowledgingConsumerAwareMessageListener<K, V> {

    private KafkaListenerErrorHandler errorHandler;

    public RecordMessagingMessageListenerAdapter(Object bean, Method method) {
        this(bean, method, null);
    }

    public RecordMessagingMessageListenerAdapter(Object bean, Method method, KafkaListenerErrorHandler errorHandler) {
        super(bean, method);
        this.errorHandler = errorHandler;
    }

    /**
     * Kafka {@link MessageListener} entry point.
     * <p> Delegate the message to the target listener method,
     * with appropriate conversion of the message argument.
     * @param record the incoming Kafka {@link ConsumerRecord}.
     * @param acknowledgment the acknowledgment.
     * @param consumer the consumer.
     */
    @Override
    public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        Message<?> message = toMessagingMessage(record, acknowledgment, consumer);
        if (logger.isDebugEnabled()) {
            logger.debug("Processing [" + message + "]");
        }
        try {
            Object result = invokeHandler(record, acknowledgment, message, consumer);
            if (result != null) {
                handleResult(result, record, message);
            }
        }
        catch (ListenerExecutionFailedException e) {
            if (this.errorHandler != null) {
                try {
                    Object result = this.errorHandler.handleError(message, e, consumer);
                    if (result != null) {
                        handleResult(result, record, message);
                    }
                }
                catch (Exception ex) {
                    throw new ListenerExecutionFailedException(createMessagingErrorMessage(
                            "Listener error handler threw an exception for the incoming message",
                            message.getPayload()), ex);
                }
            }
            else {
                throw e;
            }
        }
    }

}
