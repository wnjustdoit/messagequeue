package com.caiya.kafka.spring.boot.autoconfigure;

import com.caiya.kafka.springn.core.*;
import com.caiya.kafka.springn.listener.ListenerConsumer;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

/**
 * KafkaAutoConfiguration.
 *
 * @author wangnan
 * @since 1.2.0, 2019/12/4
 **/
@Configuration
@ConditionalOnClass({KafkaClient.class, KafkaTemplate.class})
@ConditionalOnMissingBean({ProducerFactory.class, KafkaTemplate.class, ConsumerFactory.class})
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaAutoConfiguration {

    private final KafkaProperties kafkaProperties;

    @Autowired
    public KafkaAutoConfiguration(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.producer-config", name = "bootstrap-servers")
    public ProducerFactory<String, String> producerFactory() {
        StringSerializer stringSerializer = new StringSerializer();
        return new DefaultKafkaProducerFactory<>(kafkaProperties.getProducerConfig(),
                stringSerializer, stringSerializer);
    }

    @Bean
    @ConditionalOnBean(ProducerFactory.class)
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        // you'd better set a default topic, or else some methods of KafkaTemplate cannot be used, such as sendDefault..
        if (!CollectionUtils.isEmpty(kafkaProperties.getTopics())) {
            // use the first one as the default topic
            kafkaTemplate.setDefaultTopic(kafkaProperties.getTopics().iterator().next());
        }
        return kafkaTemplate;
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.consumer-config", name = "bootstrap-servers")
    public ConsumerFactory<String, String> consumerFactory() {
        StringDeserializer stringDeserializer = new StringDeserializer();
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.getConsumerConfig(),
                stringDeserializer, stringDeserializer);
    }

    @Bean
    @ConditionalOnBean(ConsumerFactory.class)
    public ListenerConsumer<String, String> listenerConsumer() {
        return new ListenerConsumer<>(consumerFactory());
    }


}
