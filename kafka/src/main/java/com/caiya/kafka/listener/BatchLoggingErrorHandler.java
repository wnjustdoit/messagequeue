package com.caiya.kafka.listener;

import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple handler that invokes a {@link LoggingErrorHandler} for each record.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public class BatchLoggingErrorHandler implements BatchErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(BatchLoggingErrorHandler.class);

    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data) {
        StringBuilder message = new StringBuilder("Error while processing:\n");
        if (data == null) {
            message.append("null ");
        }
        else {
            Iterator<?> iterator = data.iterator();
            while (iterator.hasNext()) {
                message.append(iterator.next()).append('\n');
            }
        }
        log.error(message.substring(0, message.length() - 1), thrownException);
    }

}
