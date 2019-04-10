package com.caiya.kafka.support;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * A configuration container to represent a topic name, partition number and, optionally,
 * an initial offset for it. The initial offset can be:
 * <ul>
 * <li>{@code null} - do nothing;</li>
 * <li>positive (including {@code 0}) - seek to EITHER the absolute offset within the
 * partition or an offset relative to the current position for this consumer, depending
 * on {@link #isRelativeToCurrent()}.
 * </li>
 * <li>negative - seek to EITHER the offset relative to the current last offset within
 * the partition: {@code consumer.seekToEnd() + initialOffset} OR the relative to the
 * current offset for this consumer (if any), depending on
 * {@link #isRelativeToCurrent()}.</li>
 * </ul>
 * Offsets are applied when the container is {@code start()}ed.
 *
 * @author Artem Bilan
 * @author Gary Russell
 */
public class TopicPartitionInitialOffset {

    /**
     * Enumeration for "special" seeks.
     */
    public enum SeekPosition {

        /**
         * Seek to the beginning.
         */
        BEGINNING,

        /**
         * Seek to the end.
         */
        END
    }

    private final TopicPartition topicPartition;

    private final Long initialOffset;

    private final boolean relativeToCurrent;

    private final SeekPosition position;

    /**
     * Construct an instance with no initial offset management.
     *
     * @param topic     the topic.
     * @param partition the partition.
     */
    public TopicPartitionInitialOffset(String topic, int partition) {
        this(topic, partition, null, false);
    }

    /**
     * Construct an instance with the provided initial offset with
     * {@link #isRelativeToCurrent()} false.
     *
     * @param topic         the topic.
     * @param partition     the partition.
     * @param initialOffset the initial offset.
     * @see #TopicPartitionInitialOffset(String, int, Long, boolean)
     */
    public TopicPartitionInitialOffset(String topic, int partition, Long initialOffset) {
        this(topic, partition, initialOffset, false);
    }

    /**
     * Construct an instance with the provided initial offset.
     *
     * @param topic             the topic.
     * @param partition         the partition.
     * @param initialOffset     the initial offset.
     * @param relativeToCurrent true for the initial offset to be relative to
     *                          the current consumer position, false for a positive initial offset to
     *                          be absolute and a negative offset relative to the current end of the
     *                          partition.
     * @since 1.1
     */
    public TopicPartitionInitialOffset(String topic, int partition, Long initialOffset, boolean relativeToCurrent) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.initialOffset = initialOffset;
        this.relativeToCurrent = relativeToCurrent;
        this.position = null;
    }

    /**
     * Construct an instance with the provided {@link SeekPosition}.
     *
     * @param topic     the topic.
     * @param partition the partition.
     * @param position  {@link SeekPosition}.
     * @since 1.3
     */
    public TopicPartitionInitialOffset(String topic, int partition, SeekPosition position) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.initialOffset = null;
        this.relativeToCurrent = false;
        this.position = position;
    }

    public TopicPartition topicPartition() {
        return this.topicPartition;
    }

    public int partition() {
        return this.topicPartition.partition();
    }

    public String topic() {
        return this.topicPartition.topic();
    }

    public Long initialOffset() {
        return this.initialOffset;
    }

    public boolean isRelativeToCurrent() {
        return this.relativeToCurrent;
    }

    public SeekPosition getPosition() {
        return this.position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicPartitionInitialOffset that = (TopicPartitionInitialOffset) o;
        return Objects.equals(this.topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return this.topicPartition.hashCode();
    }

    @Override
    public String toString() {
        return "TopicPartitionInitialOffset{" +
                "topicPartition=" + this.topicPartition +
                ", initialOffset=" + this.initialOffset +
                ", relativeToCurrent=" + this.relativeToCurrent +
                (this.position == null ? "" : (", position=" + this.position.name())) +
                '}';
    }

}
