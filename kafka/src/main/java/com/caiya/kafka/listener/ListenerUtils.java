package com.caiya.kafka.listener;

import com.caiya.kafka.util.Assert;

/**
 * Listener utilities.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public final class ListenerUtils {

    private ListenerUtils() {
        super();
    }

    public static ListenerType determineListenerType(Object listener) {
        Assert.notNull(listener, "Listener cannot be null");
        ListenerType listenerType;
        if (listener instanceof AcknowledgingConsumerAwareMessageListener
                || listener instanceof BatchAcknowledgingConsumerAwareMessageListener) {
            listenerType = ListenerType.ACKNOWLEDGING_CONSUMER_AWARE;
        }
        else if (listener instanceof ConsumerAwareMessageListener
                || listener instanceof BatchConsumerAwareMessageListener) {
            listenerType = ListenerType.CONSUMER_AWARE;
        }
        else if (listener instanceof AcknowledgingMessageListener
                || listener instanceof BatchAcknowledgingMessageListener) {
            listenerType = ListenerType.ACKNOWLEDGING;
        }
        else if (listener instanceof GenericMessageListener) {
            listenerType = ListenerType.SIMPLE;
        }
        else {
            throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
        }
        return listenerType;
    }

}
