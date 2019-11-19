package com.caiya.kafka;

import com.caiya.kafka.listener.KafkaMessageListenerContainer;
import com.caiya.kafka.listener.MessageListener;
import com.caiya.kafka.listener.config.ContainerProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * KafkaTest.
 *
 * @author wangnan
 * @since 1.0
 */
public class KafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTest.class);

    @Test
    public void testAutoCommit() throws InterruptedException, ExecutionException {
        logger.info("Start auto");
        ContainerProperties containerProps = new ContainerProperties("topic1", "topic2");
        final CountDownLatch latch = new CountDownLatch(10);
        containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
            logger.info("received: " + message);
            latch.countDown();
        });
        KafkaMessageListenerContainer<Integer, String> container = createContainer(containerProps);
        container.setBeanName("testAuto");
        container.start();
        Thread.sleep(1000); // wait a bit for the container to start
        KafkaTemplate<Integer, String> template = createTemplate();
        String topic1 = "topic1";
        template.setDefaultTopic(topic1);
        template.sendDefault(0, "foo").get();
        template.sendDefault(2, "bar").get();
        template.sendDefault(0, "baz").get();
        template.sendDefault(2, "qux").get();
//        template.flush();
        latch.await(5, TimeUnit.SECONDS);
        container.stop();
        logger.info("Stop auto");
    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(
            ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<Integer, String> cf =
                new DefaultKafkaConsumerFactory<>(props);
        return new KafkaMessageListenerContainer<>(cf, containerProps);
    }

    private KafkaTemplate<Integer, String> createTemplate() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<Integer, String> pf =
                new DefaultKafkaProducerFactory<>(senderProps);
        return new KafkaTemplate<>(pf);
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094");
        String group = "group1";
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

}
