package com.caiya.kafka.springn.core;

import com.caiya.kafka.springn.support.LoggingProducerListener;
import com.caiya.kafka.springn.support.ProducerListener;
import com.caiya.kafka.springn.support.SendResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.List;
import java.util.Map;

/**
 * A template for executing high-level operations.
 *
 * @author wangnan
 * @since 1.0.0, 2019/11/8
 **/
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ProducerFactory<K, V> producerFactory;

    private final boolean autoFlush;

    private final boolean transactional;

    private final ThreadLocal<Producer<K, V>> producers = new ThreadLocal<>();

    private volatile String defaultTopic;

    private volatile ProducerListener<K, V> producerListener = new LoggingProducerListener<K, V>();


    /**
     * Create an instance using the supplied producer factory and autoFlush false.
     *
     * @param producerFactory the producer factory.
     */
    public KafkaTemplate(ProducerFactory<K, V> producerFactory) {
        this(producerFactory, false);
    }

    /**
     * Create an instance using the supplied producer factory and autoFlush setting.
     * <p>
     * Set autoFlush to {@code true} if you have configured the producer's
     * {@code linger.ms} to a non-default value and wish send operations on this template
     * to occur immediately, regardless of that setting, or if you wish to block until the
     * broker has acknowledged receipt according to the producer's {@code acks} property.
     *
     * @param producerFactory the producer factory.
     * @param autoFlush       true to flush after each send.
     * @see Producer#flush()
     */
    public KafkaTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush) {
        this.producerFactory = producerFactory;
        this.autoFlush = autoFlush;
        this.transactional = producerFactory.transactionCapable();
    }

    /**
     * The default topic for send methods where a topic is not
     * provided.
     *
     * @return the topic.
     */
    public String getDefaultTopic() {
        return this.defaultTopic;
    }

    /**
     * Set the default topic for send methods where a topic is not
     * provided.
     *
     * @param defaultTopic the topic.
     */
    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    /**
     * Set a {@link ProducerListener} which will be invoked when Kafka acknowledges
     * a send operation. By default a {@link LoggingProducerListener} is configured
     * which logs errors only.
     *
     * @param producerListener the listener; may be {@code null}.
     */
    public void setProducerListener(ProducerListener<K, V> producerListener) {
        this.producerListener = producerListener;
    }

    /**
     * Return true if this template supports transactions (has a transaction-capable
     * producer factory).
     *
     * @return true or false.
     */
    public boolean isTransactional() {
        return this.transactional;
    }

    @Override
    public ListenableFuture<SendResult<K, V>> sendDefault(V data) {
        return send(this.defaultTopic, data);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> sendDefault(K key, V data) {
        return send(this.defaultTopic, key, data);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, V data) {
        return send(this.defaultTopic, partition, key, data);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, Long timestamp, K key, V data) {
        return send(this.defaultTopic, partition, timestamp, key, data);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
        return doSend(producerRecord);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
        return doSend(producerRecord);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
        return doSend(producerRecord);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, Long timestamp, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, data);
        return doSend(producerRecord);
    }


    @Override
    public ListenableFuture<SendResult<K, V>> send(ProducerRecord<K, V> record) {
        return doSend(record);
    }


    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Producer<K, V> producer = getTheProducer();
        try {
            return producer.partitionsFor(topic);
        } finally {
            closeProducer(producer, inTransaction());
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        Producer<K, V> producer = getTheProducer();
        try {
            return producer.metrics();
        } finally {
            closeProducer(producer, inTransaction());
        }
    }

    @Override
    public <T> T execute(ProducerCallback<K, V, T> callback) {
        Producer<K, V> producer = getTheProducer();
        try {
            return callback.doInKafka(producer);
        } finally {
            closeProducer(producer, inTransaction());
        }
    }

    @Override
    public <T> T executeInTransaction(OperationsCallback<K, V, T> callback) {
        Assert.state(this.transactional, "Producer factory does not support transactions");
        Producer<K, V> producer = this.producers.get();
        Assert.state(producer == null, "Nested calls to 'executeInTransaction' are not allowed");
        producer = this.producerFactory.createProducer();

        try {
            producer.beginTransaction();
        } catch (Exception e) {
            closeProducer(producer, false);
            throw e;
        }

        this.producers.set(producer);
        T result = null;
        try {
            result = callback.doInOperations(this);
        } catch (Exception e) {
            try {
                producer.abortTransaction();
            } finally {
                this.producers.remove();
                closeProducer(producer, false);
                producer = null;
            }
        }
        if (producer != null) {
            try {
                producer.commitTransaction();
            } finally {
                closeProducer(producer, false);
                this.producers.remove();
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p><b>Note</b> It only makes sense to invoke this method if the
     * {@link ProducerFactory} serves up a singleton producer (such as the
     * {@link com.caiya.kafka.springn.core.DefaultKafkaProducerFactory}).
     */
    @Override
    public void flush() {
        Producer<K, V> producer = getTheProducer();
        try {
            producer.flush();
        } finally {
            closeProducer(producer, inTransaction());
        }
    }


    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // TODO
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        // TODO

    }

    protected void closeProducer(Producer<K, V> producer, boolean inLocalTx) {
        if (!inLocalTx) {
            producer.close();
        }
    }

    /**
     * Send the producer record.
     *
     * @param producerRecord the producer record.
     * @return a Future for the {@link RecordMetadata}.
     */
    protected ListenableFuture<SendResult<K, V>> doSend(final ProducerRecord<K, V> producerRecord) {
        if (this.transactional) {
            Assert.state(inTransaction(),
                    "No transaction is in process; "
                            + "possible solutions: run the template operation within the scope of a "
                            + "template.executeInTransaction() operation, start a transaction with @Transactional "
                            + "before invoking the template method, "
                            + "run in a transaction started by a listener container when consuming a record");
        }
        final Producer<K, V> producer = getTheProducer();
        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Sending: " + producerRecord);
        }
        final SettableListenableFuture<SendResult<K, V>> future = new SettableListenableFuture<>();
        producer.send(producerRecord, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                try {
                    if (exception == null) {
                        future.set(new SendResult<>(producerRecord, metadata));
                        if (com.caiya.kafka.springn.core.KafkaTemplate.this.producerListener != null) {
                            com.caiya.kafka.springn.core.KafkaTemplate.this.producerListener.onSuccess(producerRecord, metadata);
                        }
                        if (com.caiya.kafka.springn.core.KafkaTemplate.this.logger.isTraceEnabled()) {
                            com.caiya.kafka.springn.core.KafkaTemplate.this.logger.trace("Sent ok: " + producerRecord + ", metadata: " + metadata);
                        }
                    } else {
                        future.setException(new KafkaProducerException(producerRecord, "Failed to send", exception));
                        if (com.caiya.kafka.springn.core.KafkaTemplate.this.producerListener != null) {
                            com.caiya.kafka.springn.core.KafkaTemplate.this.producerListener.onError(producerRecord, exception);
                        }
                        if (com.caiya.kafka.springn.core.KafkaTemplate.this.logger.isDebugEnabled()) {
                            com.caiya.kafka.springn.core.KafkaTemplate.this.logger.debug("Failed to send: " + producerRecord, exception);
                        }
                    }
                } finally {
                    if (!com.caiya.kafka.springn.core.KafkaTemplate.this.transactional) {
                        closeProducer(producer, false);
                    }
                }
            }

        });
        if (this.autoFlush) {
            flush();
        }
        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Sent: " + producerRecord);
        }
        return future;
    }


    protected boolean inTransaction() {
        // TODO
        return this.transactional && this.producers.get() != null;
    }

    private Producer<K, V> getTheProducer() {
        // TODO
        if (this.transactional) {
            Producer<K, V> producer = this.producers.get();
            if (producer != null) {
                return producer;
            }
        }
        return this.producerFactory.createProducer();
    }

}
