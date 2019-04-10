package com.caiya.kafka.listener;

/**
 * Defines the listener type.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public enum ListenerType {

    /**
     * Acknowledging and consumer aware.
     */
    ACKNOWLEDGING_CONSUMER_AWARE,

    /**
     * Consumer aware.
     */
    CONSUMER_AWARE,

    /**
     * Acknowledging.
     */
    ACKNOWLEDGING,

    /**
     * Simple.
     */
    SIMPLE

}
