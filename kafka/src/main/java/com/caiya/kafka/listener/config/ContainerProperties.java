package com.caiya.kafka.listener.config;

import com.caiya.kafka.listener.*;
import com.caiya.kafka.support.LogIfLevelEnabled;
import com.caiya.kafka.support.TopicPartitionInitialOffset;
import com.caiya.kafka.task.AsyncListenableTaskExecutor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

/**
 * Contains runtime properties for a listener container.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Artem Yakshin
 * @author Johnny Lim
 */
public class ContainerProperties {

    /**
     * The default {@link #setPollTimeout(long) pollTimeout} (ms).
     */
    public static final long DEFAULT_POLL_TIMEOUT = 1000L;

    /**
     * The default {@link #setShutdownTimeout(long) shutDownTimeout} (ms).
     */
    public static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;

    /**
     * The default {@link #setMonitorInterval(int) monitorInterval} (s).
     */
    public static final int DEFAULT_MONITOR_INTERVAL = 30;

    /**
     * The default {@link #setNoPollThreshold(float) noPollThreshold}.
     */
    public static final float DEFAULT_NO_POLL_THRESHOLD = 3f;

    /**
     * Topic names.
     */
    private final String[] topics;

    /**
     * Topic pattern.
     */
    private final Pattern topicPattern;

    /**
     * Topics/partitions/initial offsets.
     */
    private final TopicPartitionInitialOffset[] topicPartitions;

    /**
     * The ack mode to use when auto ack (in the configuration properties) is false.
     * <ul>
     * <li>RECORD: Ack after each record has been passed to the listener.</li>
     * <li>BATCH: Ack after each batch of records received from the consumer has been
     * passed to the listener</li>
     * <li>TIME: Ack after this number of milliseconds; (should be greater than
     * {@code #setPollTimeout(long) pollTimeout}.</li>
     * <li>COUNT: Ack after at least this number of records have been received</li>
     * <li>MANUAL: Listener is responsible for acking - use a
     * {@link AcknowledgingMessageListener}.
     * </ul>
     */
    private AbstractMessageListenerContainer.AckMode ackMode = AbstractMessageListenerContainer.AckMode.BATCH;

    /**
     * The number of outstanding record count after which offsets should be
     * committed when {@link AbstractMessageListenerContainer.AckMode#COUNT} or {@link AbstractMessageListenerContainer.AckMode#COUNT_TIME} is being
     * used.
     */
    private int ackCount;

    /**
     * The time (ms) after which outstanding offsets should be committed when
     * {@link AbstractMessageListenerContainer.AckMode#TIME} or {@link AbstractMessageListenerContainer.AckMode#COUNT_TIME} is being used. Should be
     * larger than
     */
    private long ackTime;

    /**
     * The message listener; must be a {@link MessageListener}
     * or {@link AcknowledgingMessageListener}.
     */
    private Object messageListener;

    /**
     * The max time to block in the consumer waiting for records.
     */
    private volatile long pollTimeout = DEFAULT_POLL_TIMEOUT;

    /**
     * The executor for threads that poll the consumer.
     */
    private AsyncListenableTaskExecutor consumerTaskExecutor;

    /**
     * The error handler to call when the listener throws an exception.
     */
    private GenericErrorHandler<?> errorHandler;

    /**
     * The timeout for shutting down the container. This is the maximum amount of
     * time that the invocation to {@code #stop(Runnable)} will block for, before
     * returning.
     */
    private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    /**
     * A user defined {@link ConsumerRebalanceListener} implementation.
     */
    private ConsumerRebalanceListener consumerRebalanceListener;

    /**
     * The commit callback; by default a simple logging callback is used to log
     * success at DEBUG level and failures at ERROR level.
     */
    private OffsetCommitCallback commitCallback;

    /**
     * Whether or not to call consumer.commitSync() or commitAsync() when the
     * container is responsible for commits. Default true. See
     * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
     * writing, async commits are not entirely reliable.
     */
    private boolean syncCommits = true;

    private boolean ackOnError = true;

    private Long idleEventInterval;

    private String groupId;

    private int monitorInterval = DEFAULT_MONITOR_INTERVAL;

//    private TaskScheduler scheduler;

    private float noPollThreshold = DEFAULT_NO_POLL_THRESHOLD;

    private String clientId = "";

    private boolean logContainerConfig;

    private LogIfLevelEnabled.Level commitLogLevel = LogIfLevelEnabled.Level.DEBUG;

    public ContainerProperties(String... topics) {
        if (topics == null || topics.length == 0)
            throw new IllegalArgumentException("An array of topicPartitions must be provided");

        this.topics = Arrays.asList(topics).toArray(new String[topics.length]);
        this.topicPattern = null;
        this.topicPartitions = null;
    }

    public ContainerProperties(Pattern topicPattern) {
        this.topics = null;
        this.topicPattern = topicPattern;
        this.topicPartitions = null;
    }

    public ContainerProperties(TopicPartitionInitialOffset... topicPartitions) {
        this.topics = null;
        this.topicPattern = null;
        if (topicPartitions == null || topicPartitions.length == 0)
            throw new IllegalArgumentException("An array of topicPartitions must be provided");
        this.topicPartitions = new LinkedHashSet<>(Arrays.asList(topicPartitions))
                .toArray(new TopicPartitionInitialOffset[topicPartitions.length]);
    }

    /**
     * Set the message listener; must be a {@link MessageListener}
     * or {@link AcknowledgingMessageListener}.
     *
     * @param messageListener the listener.
     */
    public void setMessageListener(Object messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * Set the ack mode to use when auto ack (in the configuration properties) is false.
     * <ul>
     * <li>RECORD: Ack after each record has been passed to the listener.</li>
     * <li>BATCH: Ack after each batch of records received from the consumer has been
     * passed to the listener</li>
     * <li>TIME: Ack after this number of milliseconds; (should be greater than
     * {@code #setPollTimeout(long) pollTimeout}.</li>
     * <li>COUNT: Ack after at least this number of records have been received</li>
     * <li>MANUAL: Listener is responsible for acking - use a
     * {@link AcknowledgingMessageListener}.
     * </ul>
     *
     * @param ackMode the {@link AbstractMessageListenerContainer.AckMode}; default BATCH.
     */
    public void setAckMode(AbstractMessageListenerContainer.AckMode ackMode) {
        if (ackMode == null)
            throw new IllegalArgumentException("'ackMode' cannot be null");
        this.ackMode = ackMode;
    }

    /**
     * Set the max time to block in the consumer waiting for records.
     *
     * @param pollTimeout the timeout in ms; default {@value #DEFAULT_POLL_TIMEOUT}.
     */
    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    /**
     * Set the number of outstanding record count after which offsets should be
     * committed when {@link AbstractMessageListenerContainer.AckMode#COUNT} or {@link AbstractMessageListenerContainer.AckMode#COUNT_TIME} is being used.
     *
     * @param count the count
     */
    public void setAckCount(int count) {
        if (count <= 0)
            throw new IllegalArgumentException("'ackCount' must be > 0");
        this.ackCount = count;
    }

    /**
     * Set the time (ms) after which outstanding offsets should be committed when
     * {@link AbstractMessageListenerContainer.AckMode#TIME} or {@link AbstractMessageListenerContainer.AckMode#COUNT_TIME} is being used. Should be
     * larger than
     *
     * @param ackTime the time
     */
    public void setAckTime(long ackTime) {
        if (ackTime <= 0)
            throw new IllegalArgumentException("'ackCount' must be > 0");
        this.ackTime = ackTime;
    }

    /**
     * Set the error handler to call when the listener throws an exception.
     *
     * @param errorHandler the error handler.
     */
    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    /**
     * Set the batch error handler to call when the listener throws an exception.
     *
     * @param errorHandler the error handler.
     */
    public void setBatchErrorHandler(BatchErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    /**
     * Set the executor for threads that poll the consumer.
     *
     * @param consumerTaskExecutor the executor
     */
    public void setConsumerTaskExecutor(AsyncListenableTaskExecutor consumerTaskExecutor) {
        this.consumerTaskExecutor = consumerTaskExecutor;
    }

    /**
     * Set the timeout for shutting down the container. This is the maximum amount of
     * time that the invocation to {@code #stop(Runnable)} will block for, before
     * returning; default {@value #DEFAULT_SHUTDOWN_TIMEOUT}.
     *
     * @param shutdownTimeout the shutdown timeout.
     */
    public void setShutdownTimeout(long shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    /**
     * Set the user defined {@link ConsumerRebalanceListener} implementation.
     *
     * @param consumerRebalanceListener the {@link ConsumerRebalanceListener} instance
     */
    public void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
        this.consumerRebalanceListener = consumerRebalanceListener;
    }

    /**
     * Set the commit callback; by default a simple logging callback is used to log
     * success at DEBUG level and failures at ERROR level.
     *
     * @param commitCallback the callback.
     */
    public void setCommitCallback(OffsetCommitCallback commitCallback) {
        this.commitCallback = commitCallback;
    }

    /**
     * Set whether or not to call consumer.commitSync() or commitAsync() when the
     * container is responsible for commits. Default true. See
     * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
     * writing, async commits are not entirely reliable.
     *
     * @param syncCommits true to use commitSync().
     */
    public void setSyncCommits(boolean syncCommits) {
        this.syncCommits = syncCommits;
    }

    /**
     * Set the idle event interval; when set, an event is emitted if a poll returns
     * no records and this interval has elapsed since a record was returned.
     *
     * @param idleEventInterval the interval.
     */
    public void setIdleEventInterval(Long idleEventInterval) {
        this.idleEventInterval = idleEventInterval;
    }

    /**
     * Set whether or not the container should commit offsets (ack messages) where the
     * listener throws exceptions. This works in conjunction with {@link #ackMode} and is
     * effective only when the kafka property {@code enable.auto.commit} is {@code false};
     * it is not applicable to manual ack modes. When this property is set to {@code true}
     * (the default), all messages handled will have their offset committed. When set to
     * {@code false}, offsets will be committed only for successfully handled messages.
     * Manual acks will always be applied. Bear in mind that, if the next message is
     * successfully handled, its offset will be committed, effectively committing the
     * offset of the failed message anyway, so this option has limited applicability.
     * Perhaps useful for a component that starts throwing exceptions consistently;
     * allowing it to resume when restarted from the last successfully processed message.
     * <p>
     * Does not apply when transactions are used - in that case, whether or not the
     * offsets are sent to the transaction depends on whether the transaction is committed
     * or rolled back. If a listener throws an exception, the transaction will normally
     * be rolled back unless an error handler is provided that handles the error and
     * exits normally; in which case the offsets are sent to the transaction and the
     * transaction is committed.
     *
     * @param ackOnError whether the container should acknowledge messages that throw
     *                   exceptions.
     */
    public void setAckOnError(boolean ackOnError) {
        this.ackOnError = ackOnError;
    }

    /**
     * Set the group id for this container. Overrides any {@code group.id} property
     * provided by the consumer factory configuration.
     *
     * @param groupId the group id.
     * @since 1.3
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String[] getTopics() {
        return this.topics;
    }

    public Pattern getTopicPattern() {
        return this.topicPattern;
    }

    public TopicPartitionInitialOffset[] getTopicPartitions() {
        return this.topicPartitions;
    }

    public AbstractMessageListenerContainer.AckMode getAckMode() {
        return this.ackMode;
    }

    public int getAckCount() {
        return this.ackCount;
    }

    public long getAckTime() {
        return this.ackTime;
    }

    public Object getMessageListener() {
        return this.messageListener;
    }

    public long getPollTimeout() {
        return this.pollTimeout;
    }

    public AsyncListenableTaskExecutor getConsumerTaskExecutor() {
        return this.consumerTaskExecutor;
    }

    public GenericErrorHandler<?> getGenericErrorHandler() {
        return this.errorHandler;
    }

    public long getShutdownTimeout() {
        return this.shutdownTimeout;
    }

    public ConsumerRebalanceListener getConsumerRebalanceListener() {
        return this.consumerRebalanceListener;
    }

    public OffsetCommitCallback getCommitCallback() {
        return this.commitCallback;
    }

    public boolean isSyncCommits() {
        return this.syncCommits;
    }

    public Long getIdleEventInterval() {
        return this.idleEventInterval;
    }

    public boolean isAckOnError() {
        return this.ackOnError &&
                !(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE.equals(this.ackMode) || AbstractMessageListenerContainer.AckMode.MANUAL.equals(this.ackMode));
    }

    public String getGroupId() {
        return this.groupId;
    }

    public int getMonitorInterval() {
        return this.monitorInterval;
    }

    /**
     * The interval between checks for a non-responsive consumer in
     * seconds; default {@value #DEFAULT_MONITOR_INTERVAL}.
     *
     * @param monitorInterval the interval.
     * @since 1.3.1
     */
    public void setMonitorInterval(int monitorInterval) {
        this.monitorInterval = monitorInterval;
    }

//    public TaskScheduler getScheduler() {
//        return this.scheduler;
//    }

    /**
     * A scheduler used with the monitor interval.
     * <p>
     * //     * @param scheduler the scheduler.
     *
     * @see #setMonitorInterval(int)
     * @since 1.3.1
     */
//    public void setScheduler(TaskScheduler scheduler) {
//        this.scheduler = scheduler;
//    }
    public float getNoPollThreshold() {
        return this.noPollThreshold;
    }

    /**
     * If the time since the last poll / {@link #getPollTimeout() poll timeout}
     * exceeds this value, a NonResponsiveConsumerEvent is published.
     * Default {@value #DEFAULT_NO_POLL_THRESHOLD}.
     *
     * @param noPollThreshold the threshold
     * @since 1.3.1
     */
    public void setNoPollThreshold(float noPollThreshold) {
        this.noPollThreshold = noPollThreshold;
    }

    /**
     * Return the client id.
     *
     * @return the client id.
     * @see #setClientId(String)
     * @since 2.1.1
     */
    public String getClientId() {
        return this.clientId;
    }

    /**
     * Set the client id; overrides the consumer factory client.id property.
     * When used in a concurrent container, will be suffixed with '-n' to
     * provide a unique value for each consumer.
     *
     * @param clientId the client id.
     * @since 2.1.1
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Log the container configuration if true (INFO).
     *
     * @return true to log.
     * @since 2.1.1
     */
    public boolean isLogContainerConfig() {
        return this.logContainerConfig;
    }

    /**
     * Set to true to instruct each container to log this configuration.
     *
     * @param logContainerConfig true to log.
     * @since 2.1.1
     */
    public void setLogContainerConfig(boolean logContainerConfig) {
        this.logContainerConfig = logContainerConfig;
    }

    /**
     * The level at which to log offset commits.
     *
     * @return the level.
     * @since 2.1.2
     */
    public LogIfLevelEnabled.Level getCommitLogLevel() {
        return this.commitLogLevel;
    }

    /**
     * Set the level at which to log offset commits.
     * Default: DEBUG.
     *
     * @param commitLogLevel the level.
     * @since 2.1.2
     */
    public void setCommitLogLevel(LogIfLevelEnabled.Level commitLogLevel) {
        if (commitLogLevel == null)
            throw new IllegalArgumentException("'commitLogLevel' cannot be null");

        this.commitLogLevel = commitLogLevel;
    }

    @Override
    public String toString() {
        return "ContainerProperties ["
                + (this.topics != null ? "topics=" + Arrays.toString(this.topics) : "")
                + (this.topicPattern != null ? ", topicPattern=" + this.topicPattern : "")
                + (this.topicPartitions != null
                ? ", topicPartitions=" + Arrays.toString(this.topicPartitions) : "")
                + ", ackMode=" + this.ackMode
                + ", ackCount=" + this.ackCount
                + ", ackTime=" + this.ackTime
                + ", messageListener=" + this.messageListener
                + ", pollTimeout=" + this.pollTimeout
                + (this.consumerTaskExecutor != null
                ? ", consumerTaskExecutor=" + this.consumerTaskExecutor : "")
                + (this.errorHandler != null ? ", errorHandler=" + this.errorHandler : "")
                + ", shutdownTimeout=" + this.shutdownTimeout
                + (this.consumerRebalanceListener != null
                ? ", consumerRebalanceListener=" + this.consumerRebalanceListener : "")
                + (this.commitCallback != null ? ", commitCallback=" + this.commitCallback : "")
                + ", syncCommits=" + this.syncCommits
                + ", ackOnError=" + this.ackOnError
                + ", idleEventInterval="
                + (this.idleEventInterval == null ? "not enabled" : this.idleEventInterval)
                + (this.groupId != null ? ", groupId=" + this.groupId : "")
                + ", monitorInterval=" + this.monitorInterval
//                + (this.scheduler != null ? ", scheduler=" + this.scheduler : "")
                + ", noPollThreshold=" + this.noPollThreshold
                + (this.clientId != null && !this.clientId.isEmpty() ? ", clientId=" + this.clientId : "")
                + "]";
    }

}
