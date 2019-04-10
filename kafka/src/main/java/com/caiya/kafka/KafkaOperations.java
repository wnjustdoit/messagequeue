package com.caiya.kafka;

import java.util.List;
import java.util.Map;

import com.caiya.kafka.support.SendResult;
import com.caiya.kafka.util.concurrent.ListenableFuture;
import com.caiya.kafka.support.KafkaHeaders;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.messaging.Message;

/**
 * The basic Kafka operations contract returning {@link ListenableFuture}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *            <p>
 *            If the Kafka topic is set with {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}
 *            all send operations will use the user provided time if provided, else
 *            {@link org.apache.kafka.clients.producer.KafkaProducer} will generate one
 *            <p>
 *            If the topic is set with {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}
 *            then the user provided timestamp will be ignored and instead will be the
 *            Kafka broker local time when the message is appended
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Biju Kunjummen
 */
public interface KafkaOperations<K, V> {

    /**
     * Send the data to the default topic with no key or partition.
     *
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> sendDefault(V data);

    /**
     * Send the data to the default topic with the provided key and no partition.
     *
     * @param key  the key.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> sendDefault(K key, V data);

    /**
     * Send the data to the default topic with the provided key and partition.
     *
     * @param partition the partition.
     * @param key       the key.
     * @param data      the data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, V data);

    /**
     * Send the data to the default topic with the provided key and partition.
     *
     * @param partition the partition.
     * @param timestamp the timestamp of the record.
     * @param key       the key.
     * @param data      the data.
     * @return a Future for the {@link SendResult}.
     * @since 1.3
     */
    ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, Long timestamp, K key, V data);

    /**
     * Send the data to the provided topic with no key or partition.
     *
     * @param topic the topic.
     * @param data  The data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> send(String topic, V data);

    /**
     * Send the data to the provided topic with the provided key and no partition.
     *
     * @param topic the topic.
     * @param key   the key.
     * @param data  The data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> send(String topic, K key, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     *
     * @param topic     the topic.
     * @param partition the partition.
     * @param key       the key.
     * @param data      the data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     *
     * @param topic     the topic.
     * @param partition the partition.
     * @param timestamp the timestamp of the record.
     * @param key       the key.
     * @param data      the data.
     * @return a Future for the {@link SendResult}.
     * @since 1.3
     */
    ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, Long timestamp, K key, V data);

    /**
     * Send the provided {@link ProducerRecord}.
     *
     * @param record the record.
     * @return a Future for the {@link SendResult}.
     * @since 1.3
     */
    ListenableFuture<SendResult<K, V>> send(ProducerRecord<K, V> record);

    /**
     * Send a message with routing information in message headers. The message payload
     * may be converted before sending.
     *
     * @param message the message to send.
     * @return a Future for the {@link SendResult}.
     * @see KafkaHeaders#TOPIC
     * @see KafkaHeaders#PARTITION_ID
     * @see KafkaHeaders#MESSAGE_KEY
     */
    ListenableFuture<SendResult<K, V>> send(Message<?> message);

    /**
     * See {@link Producer#partitionsFor(String)}.
     *
     * @param topic the topic.
     * @return the partition info.
     * @since 1.1
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * See {@link Producer#metrics()}.
     *
     * @return the metrics.
     * @since 1.1
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * Execute some arbitrary operation(s) on the producer and return the result.
     *
     * @param callback the callback.
     * @param <T>      the result type.
     * @return the result.
     * @since 1.1
     */
    <T> T execute(ProducerCallback<K, V, T> callback);

    /**
     * Flush the producer.
     */
    void flush();

    /**
     * A callback for executing arbitrary operations on the {@link Producer}.
     *
     * @param <K> the key type.
     * @param <V> the value type.
     * @param <T> the return type.
     * @since 1.3
     */
    interface ProducerCallback<K, V, T> {

        T doInKafka(Producer<K, V> producer);

    }

    /**
     * A callback for executing arbitrary operations on the {@link KafkaOperations}.
     *
     * @param <K> the key type.
     * @param <V> the value type.
     * @param <T> the return type.
     * @since 1.3
     */
    interface OperationsCallback<K, V, T> {

        T doInOperations(KafkaOperations<K, V> operations);

    }

}
