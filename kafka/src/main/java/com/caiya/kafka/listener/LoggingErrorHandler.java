package com.caiya.kafka.listener;

import com.caiya.kafka.util.ObjectUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link ErrorHandler} implementation for logging purpose.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class LoggingErrorHandler implements ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingErrorHandler.class);

    @Override
    public void handle(Exception thrownException, ConsumerRecord<?, ?> record) {
        log.error("Error while processing: " + ObjectUtils.nullSafeToString(record), thrownException);
    }

}
