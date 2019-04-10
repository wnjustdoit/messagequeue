package com.caiya.kafka.support;

import com.caiya.kafka.util.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ProducerListener} that logs exceptions thrown when sending messages.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class LoggingProducerListener<K, V> implements ProducerListener<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(LoggingProducerListener.class);

    private boolean includeContents = true;

    private int maxContentLogged = 100;

    /**
     * Whether the log message should include the contents (key and payload).
     *
     * @param includeContents true if the contents of the message should be logged
     */
    public void setIncludeContents(boolean includeContents) {
        this.includeContents = includeContents;
    }

    /**
     * The maximum amount of data to be logged for either key or password. As message sizes may vary and
     * become fairly large, this allows limiting the amount of data sent to logs.
     *
     * @param maxContentLogged the maximum amount of data being logged.
     */
    public void setMaxContentLogged(int maxContentLogged) {
        this.maxContentLogged = maxContentLogged;
    }

    @Override
    public void onError(String topic, Integer partition, K key, V value, Exception exception) {
        if (logger.isErrorEnabled()) {
            StringBuilder logOutput = new StringBuilder();
            logOutput.append("Exception thrown when sending a message");
            if (this.includeContents) {
                logOutput.append(" with key='").append(toDisplayString(ObjectUtils.nullSafeToString(key), this.maxContentLogged)).append("'");
                logOutput.append(" and payload='").append(toDisplayString(ObjectUtils.nullSafeToString(value), this.maxContentLogged)).append("'");
            }
            logOutput.append(" to topic ").append(topic);
            if (partition != null) {
                logOutput.append(" and partition ").append(partition);
            }
            logOutput.append(":");
            logger.error(logOutput.toString(), exception);
        }
    }

    private String toDisplayString(String original, int maxCharacters) {
        if (original.length() <= maxCharacters) {
            return original;
        }
        return original.substring(0, maxCharacters) + "...";
    }

}
