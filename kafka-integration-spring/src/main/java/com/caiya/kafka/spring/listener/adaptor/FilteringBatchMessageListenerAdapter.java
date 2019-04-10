package com.caiya.kafka.spring.listener.adaptor;

import java.util.Iterator;
import java.util.List;

import com.caiya.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import com.caiya.kafka.listener.BatchMessageListener;
import com.caiya.kafka.listener.ListenerType;
import com.caiya.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A {@link BatchMessageListener} adapter that implements filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class FilteringBatchMessageListenerAdapter<K, V>
        extends AbstractFilteringMessageListener<K, V, BatchMessageListener<K, V>>
        implements BatchAcknowledgingConsumerAwareMessageListener<K, V> {

    private final boolean ackDiscarded;

    /**
     * Create an instance with the supplied strategy and delegate listener.
     * @param delegate the delegate.
     * @param recordFilterStrategy the filter.
     */
    public FilteringBatchMessageListenerAdapter(BatchMessageListener<K, V> delegate,
                                                RecordFilterStrategy<K, V> recordFilterStrategy) {
        super(delegate, recordFilterStrategy);
        this.ackDiscarded = false;
    }

    /**
     * Create an instance with the supplied strategy and delegate listener.
     * When 'ackDiscarded' is false, and all messages are filtered, an empty list
     * is passed to the delegate (so it can decide whether or not to ack); when true, a
     * completely filtered batch is ack'd by this class, and no call is made to the delegate.
     * @param delegate the delegate.
     * @param recordFilterStrategy the filter.
     * @param ackDiscarded true to ack (commit offset for) discarded messages when the
     * listener is configured for manual acks.
     */
    public FilteringBatchMessageListenerAdapter(BatchMessageListener<K, V> delegate,
                                                RecordFilterStrategy<K, V> recordFilterStrategy, boolean ackDiscarded) {
        super(delegate, recordFilterStrategy);
        this.ackDiscarded = ackDiscarded;
    }

    @Override
    public void onMessage(List<ConsumerRecord<K, V>> consumerRecords, Acknowledgment acknowledgment,
                          Consumer<?, ?> consumer) {
        Iterator<ConsumerRecord<K, V>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            if (filter(iterator.next())) {
                iterator.remove();
            }
        }
        if (consumerRecords.size() > 0 || this.delegateType.equals(ListenerType.ACKNOWLEDGING_CONSUMER_AWARE)
                || this.delegateType.equals(ListenerType.CONSUMER_AWARE)
                || (!this.ackDiscarded && this.delegateType.equals(ListenerType.ACKNOWLEDGING))) {
            switch (this.delegateType) {
                case ACKNOWLEDGING_CONSUMER_AWARE:
                    this.delegate.onMessage(consumerRecords, acknowledgment, consumer);
                    break;
                case ACKNOWLEDGING:
                    this.delegate.onMessage(consumerRecords, acknowledgment);
                    break;
                case CONSUMER_AWARE:
                    this.delegate.onMessage(consumerRecords, consumer);
                    break;
                case SIMPLE:
                    this.delegate.onMessage(consumerRecords);
            }
        }
        else {
            if (this.ackDiscarded && acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */

    @Override
    public void onMessage(List<ConsumerRecord<K, V>> data) {
        onMessage(data, null, null);
    }

    @Override
    public void onMessage(List<ConsumerRecord<K, V>> data, Acknowledgment acknowledgment) {
        onMessage(data, acknowledgment, null);
    }

    @Override
    public void onMessage(List<ConsumerRecord<K, V>> data, Consumer<?, ?> consumer) {
        onMessage(data, null, consumer);
    }

}
