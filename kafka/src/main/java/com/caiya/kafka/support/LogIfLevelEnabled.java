package com.caiya.kafka.support;

import org.slf4j.Logger;

import java.util.function.Supplier;

/**
 * Wrapper for a Simple Logging Facade for Java Log supporting configurable
 * logging levels.
 *
 * @author Gary Russell
 * @since 2.1.2
 */
public final class LogIfLevelEnabled {

    private final Logger logger;

    private final Level level;

    public LogIfLevelEnabled(Logger logger, Level level) {
        if (logger == null)
            throw new IllegalArgumentException("'logger' cannot be null");
        if (level == null)
            throw new IllegalArgumentException("'level' cannot be null");
        this.logger = logger;
        this.level = level;
    }

    /**
     * Logging levels.
     */
    public enum Level {

        /**
         * Error.
         */
        ERROR,

        /**
         * Warn.
         */
        WARN,

        /**
         * Info.
         */
        INFO,

        /**
         * Debug.
         */
        DEBUG,

        /**
         * Trace.
         */
        TRACE

    }

    public void log(Supplier<Object> messageSupplier) {
        switch (this.level) {
            case ERROR:
                if (this.logger.isErrorEnabled()) {
                    this.logger.error(messageSupplier.get().toString(), (Object) null);
                }
                break;
            case WARN:
                if (this.logger.isWarnEnabled()) {
                    this.logger.warn(messageSupplier.get().toString(), (Object) null);
                }
                break;
            case INFO:
                if (this.logger.isInfoEnabled()) {
                    this.logger.info(messageSupplier.get().toString(), (Object) null);
                }
                break;
            case DEBUG:
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug(messageSupplier.get().toString(), (Object) null);
                }
                break;
            case TRACE:
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace(messageSupplier.get().toString(), (Object) null);
                }
                break;
        }
    }

    public void log(Supplier<Object> messageSupplier, Throwable t) {
        switch (this.level) {
            case ERROR:
                if (this.logger.isErrorEnabled()) {
                    this.logger.error(messageSupplier.get().toString(), t);
                }
                break;
            case WARN:
                if (this.logger.isWarnEnabled()) {
                    this.logger.warn(messageSupplier.get().toString(), t);
                }
                break;
            case INFO:
                if (this.logger.isInfoEnabled()) {
                    this.logger.info(messageSupplier.get().toString(), t);
                }
                break;
            case DEBUG:
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug(messageSupplier.get().toString(), t);
                }
                break;
            case TRACE:
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace(messageSupplier.get().toString(), t);
                }
                break;
        }
    }

}
