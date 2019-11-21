package com.caiya.kafka.springn.listener;

import com.caiya.kafka.springn.core.ConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The entrance of Consumers' messaging.
 *
 * @author wangnan
 * @since 1.0.0, 2019/11/19
 **/
@Component
@ConditionalOnBean(ConsumerFactory.class)
public class ListenerConsumer<K, V> implements ApplicationContextAware, InitializingBean, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(ListenerConsumer.class);

    private static final int MAX_MESSAGING_THREAD_COUNT = 20;

    private static final AtomicBoolean RUNNABLE = new AtomicBoolean(true);

    private ApplicationContext applicationContext;

    private final ConsumerFactory<K, V> defaultConsumerFactory;

    @Autowired
    public ListenerConsumer(ConsumerFactory<K, V> consumerFactory) {
        this.defaultConsumerFactory = consumerFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.boot();
    }


    private void boot() {
        Map<String, GenericMessageListener> messageListenerMap = applicationContext.getBeansOfType(GenericMessageListener.class);
        if (messageListenerMap != null) {
            if (messageListenerMap.values().size() > MAX_MESSAGING_THREAD_COUNT) {
                logger.warn("message listener number exceed MAX_MESSAGING_THREAD_COUNT[" + MAX_MESSAGING_THREAD_COUNT + "]");
                return;
            }
            messageListenerMap.values().forEach(messageListener -> {
                if (!CollectionUtils.isEmpty(messageListener.topics())) {
                    logger.info("start to process message listener:" + messageListener);
                    new Thread(new KafkaConsumerRunner(messageListener)).start();
                } else {
                    logger.warn("kafka topics cannot be empty, message listener:" + messageListener);
                }
            });
        }
    }

    class KafkaConsumerRunner implements Runnable {

        private final GenericMessageListener messageListener;

        KafkaConsumerRunner(GenericMessageListener messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Consumer<K, V> consumer;
            if (StringUtils.hasText(messageListener.consumerFactoryName())) {
                consumer = ((ConsumerFactory) applicationContext.getBean(messageListener.consumerFactoryName())).createConsumer();
            } else {
                consumer = defaultConsumerFactory.createConsumer();
            }
            if (CollectionUtils.isEmpty(messageListener.partitions())) {
                consumer.subscribe(messageListener.topics());
            } else {
                // partition subscription
                if (messageListener.topics().size() > 1) {
                    throw new IllegalArgumentException("Multi-topics not allowed in Partition subscription");
                }
                String topic = (String) messageListener.topics().iterator().next();
                List<TopicPartition> topicPartitions = new ArrayList<>();
                messageListener.partitions().forEach(partition -> {
                    topicPartitions.add(new TopicPartition(topic, (int) partition));
                });
                consumer.assign(topicPartitions);
            }
            while (RUNNABLE.get()) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(messageListener.pollTimeoutInMillis());
                if (messageListener instanceof MessageListener) {
                    messageListener.onMessage(consumerRecords);
                } else if (messageListener instanceof AcknowledgingMessageListener) {
                    messageListener.onMessage(consumerRecords, consumer);
                } else {
                    throw new UnsupportedOperationException("Unsupported message listener type:" + messageListener);
                }
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        RUNNABLE.compareAndSet(true, false);
    }


}
