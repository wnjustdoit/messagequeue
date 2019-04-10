package com.caiya.kafka.support;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.springframework.messaging.MessageHeaders;

/**
 * Header mapper for Apache Kafka.
 *
 * @author Gary Russell
 * @since 1.3
 */
public interface KafkaHeaderMapper {

    /**
     * Map from the given {@link MessageHeaders} to the specified target message.
     *
     * @param headers the abstracted MessageHeaders.
     * @param target  the native target message.
     */
    void fromHeaders(MessageHeaders headers, Headers target);

    /**
     * Map from the given target message to abstracted {@link MessageHeaders}.
     *
     * @param source the native target message.
     * @param target the target headers.
     */
    void toHeaders(Headers source, Map<String, Object> target);

}
