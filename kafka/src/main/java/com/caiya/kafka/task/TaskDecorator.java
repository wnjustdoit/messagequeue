package com.caiya.kafka.task;

import java.util.concurrent.Executor;

/**
 * A callback interface for a decorator to be applied to any {@link Runnable}
 * about to be executed.
 * <p>
 * <p>Note that such a decorator is not necessarily being applied to the
 * user-supplied {@code Runnable}/{@code Callable} but rather to the actual
 * execution callback (which may be a wrapper around the user-supplied task).
 * <p>
 * <p>The primary use case is to set some execution context around the task's
 * invocation, or to provide some monitoring/statistics for task execution.
 *
 * @author Juergen Hoeller
 * @see Executor#execute(Runnable)
 * @see SimpleAsyncTaskExecutor#setTaskDecorator
 * @since 4.3
 */
public interface TaskDecorator {

    /**
     * Decorate the given {@code Runnable}, returning a potentially wrapped
     * {@code Runnable} for actual execution.
     *
     * @param runnable the original {@code Runnable}
     * @return the decorated {@code Runnable}
     */
    Runnable decorate(Runnable runnable);

}
