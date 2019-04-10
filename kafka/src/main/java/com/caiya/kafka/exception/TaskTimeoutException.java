package com.caiya.kafka.exception;

import com.caiya.kafka.task.AsyncTaskExecutor;

/**
 * Exception thrown when a {@link AsyncTaskExecutor} rejects to accept
 * a given task for execution because of the specified timeout.
 *
 * @author Juergen Hoeller
 * @see AsyncTaskExecutor#execute(Runnable, long)
 * @see TaskRejectedException
 * @since 2.0.3
 */
@SuppressWarnings("serial")
public class TaskTimeoutException extends TaskRejectedException {

    /**
     * Create a new {@code TaskTimeoutException}
     * with the specified detail message and no root cause.
     *
     * @param msg the detail message
     */
    public TaskTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Create a new {@code TaskTimeoutException}
     * with the specified detail message and the given root cause.
     *
     * @param msg   the detail message
     * @param cause the root cause (usually from using an underlying
     *              API such as the {@code java.util.concurrent} package)
     * @see java.util.concurrent.RejectedExecutionException
     */
    public TaskTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
