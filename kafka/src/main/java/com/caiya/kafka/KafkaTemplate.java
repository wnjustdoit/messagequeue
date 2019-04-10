package com.caiya.kafka;

import java.util.List;
import java.util.Map;

import com.caiya.kafka.exception.KafkaProducerException;
import com.caiya.kafka.support.*;
import com.caiya.kafka.support.converter.MessagingMessageConverter;
import com.caiya.kafka.support.converter.RecordMessageConverter;
import com.caiya.kafka.util.concurrent.ListenableFuture;
import com.caiya.kafka.util.concurrent.SettableListenableFuture;
import com.caiya.kafka.support.converter.MessageConverter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;


/**
 * A template for executing high-level operations.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Biju Kunjummen
 * @author Endika Gutiérrez
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass()); //NOSONAR

    private final ProducerFactory<K, V> producerFactory;

    private final boolean autoFlush;

    private RecordMessageConverter messageConverter = new MessagingMessageConverter();

    private volatile String defaultTopic;

    private volatile ProducerListener<K, V> producerListener = new LoggingProducerListener<>();


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
     * Return the message converter.
     *
     * @return the message converter.
     */
    public MessageConverter getMessageConverter() {
        return this.messageConverter;
    }

    /**
     * Set the message converter to use.
     *
     * @param messageConverter the message converter.
     */
    public void setMessageConverter(RecordMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
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

    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<SendResult<K, V>> send(Message<?> message) {
        ProducerRecord<?, ?> producerRecord = this.messageConverter.fromMessage(message, this.defaultTopic);
        if (!producerRecord.headers().iterator().hasNext()) { // possibly no Jackson
            byte[] correlationId = message.getHeaders().get(KafkaHeaders.CORRELATION_ID, byte[].class);
            if (correlationId != null) {
                producerRecord.headers().add(KafkaHeaders.CORRELATION_ID, correlationId);
            }
        }
        return doSend((ProducerRecord<K, V>) producerRecord);
    }


    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Producer<K, V> producer = getTheProducer();
        try {
            return producer.partitionsFor(topic);
        } finally {
            closeProducer(producer);
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        Producer<K, V> producer = getTheProducer();
        try {
            return producer.metrics();
        } finally {
            closeProducer(producer);
        }
    }

    @Override
    public <T> T execute(ProducerCallback<K, V, T> callback) {
        Producer<K, V> producer = getTheProducer();
        try {
            return callback.doInKafka(producer);
        } finally {
            closeProducer(producer);
        }
    }

    /**
     * {@inheritDoc}
     * <p><b>Note</b> It only makes sense to invoke this method if the
     * {@link ProducerFactory} serves up a singleton producer (such as the
     * {@link DefaultKafkaProducerFactory}).
     */
    @Override
    public void flush() {
        Producer<K, V> producer = getTheProducer();
        try {
            producer.flush();
        } finally {
            closeProducer(producer);
        }
    }


    protected void closeProducer(Producer<K, V> producer) {
        producer.close();
    }

    /**
     * Send the producer record.
     *
     * @param producerRecord the producer record.
     * @return a Future for the {@link RecordMetadata}.
     */
    protected ListenableFuture<SendResult<K, V>> doSend(final ProducerRecord<K, V> producerRecord) {
        final Producer<K, V> producer = getTheProducer();
        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Sending: " + producerRecord);
        }
        final SettableListenableFuture<SendResult<K, V>> future = new SettableListenableFuture<>();
        producer.send(producerRecord, (metadata, exception) -> {
            try {
                if (exception == null) {
                    future.set(new SendResult<>(producerRecord, metadata));
                    if (KafkaTemplate.this.producerListener != null) {
                        KafkaTemplate.this.producerListener.onSuccess(producerRecord, metadata);
                    }
                    if (KafkaTemplate.this.logger.isTraceEnabled()) {
                        KafkaTemplate.this.logger.trace("Sent ok: " + producerRecord + ", metadata: " + metadata);
                    }
                } else {
                    future.setException(new KafkaProducerException(producerRecord, "Failed to send", exception));
                    if (KafkaTemplate.this.producerListener != null) {
                        KafkaTemplate.this.producerListener.onError(producerRecord, exception);
                    }
                    if (KafkaTemplate.this.logger.isDebugEnabled()) {
                        KafkaTemplate.this.logger.debug("Failed to send: " + producerRecord, exception);
                    }
                }
            } finally {
                closeProducer(producer);
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


    private Producer<K, V> getTheProducer() {
        return this.producerFactory.createProducer();
    }

}
