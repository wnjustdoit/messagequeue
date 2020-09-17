package com.caiya.kafka.springn.core;

import com.caiya.kafka.springn.support.SendResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;

/**
 * The basic Kafka operations contract returning {@link ListenableFuture}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author wangnan
 * @since 1.0.0, 2019/11/8
 **/
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
     * Execute some arbitrary operation(s) on the operations and return the result.
     * The operations are invoked within a local transaction and do not participate
     * in a global transaction (if present).
     *
     * @param callback the callback.
     * @param <T>      the result type.
     * @return the result.
     * @since 1.1
     */
    <T> T executeInTransaction(OperationsCallback<K, V, T> callback);

    /**
     * Flush the producer.
     */
    void flush();

    /**
     * When running in a transaction (usually synchronized with some other transaction),
     * send the consumer offset(s) to the transaction. The group id is obtained from
     * {@link ProducerFactoryUtils#getConsumerGroupId()}. It is not necessary to call
     * this method if the operations are invoked on a listener container thread since the
     * container will take care of sending the offsets to the transaction.
     *
     * @param offsets The offsets.
     * @since 1.3
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * When running in a transaction (usually synchronized with some other transaction),
     * send the consumer offset(s) to the transaction. It is not necessary to call this
     * method if the operations are invoked on a listener container thread since the
     * container will take care of sending the offsets to the transaction.
     *
     * @param offsets         The offsets.
     * @param consumerGroupId the consumer's group.id.
     * @since 1.3
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId);

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
     * A callback for executing arbitrary operations on the {@link com.caiya.kafka.springn.core.KafkaOperations}.
     *
     * @param <K> the key type.
     * @param <V> the value type.
     * @param <T> the return type.
     * @since 1.3
     */
    interface OperationsCallback<K, V, T> {

        T doInOperations(com.caiya.kafka.springn.core.KafkaOperations<K, V> operations);

    }

}
