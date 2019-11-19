package com.caiya.kafka.springn.configuration;

import com.caiya.kafka.springn.component.KafkaProperties;
import com.caiya.kafka.springn.core.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;

/**
 * Kafka相关配置（自动提交）.
 *
 * @author wangnan
 * @since 1.0
 */
@Configuration
public class KafkaAutoCommitConfiguration {

    @Resource
    private KafkaProperties autoCommitKafkaProperties;

    @Bean
    @ConfigurationProperties(prefix = "kafka.test-auto")
    public KafkaProperties autoCommitKafkaProperties() {
        return new KafkaProperties();
    }

    @Bean
    public ProducerFactory<String, String> autoCommitProducerFactory() {
        StringSerializer stringSerializer = new StringSerializer();
        return new DefaultKafkaProducerFactory<>(autoCommitKafkaProperties.getProducerConfig(),
                stringSerializer, stringSerializer);
    }

    @Bean
    public KafkaTemplate<String, String> autoCommitKafkaTemplate(ProducerFactory<String, String> autoCommitProducerFactory) {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(autoCommitProducerFactory);
        if (!CollectionUtils.isEmpty(autoCommitKafkaProperties.getTopics())) {
            // use the first one as the default topic
            kafkaTemplate.setDefaultTopic(autoCommitKafkaProperties.getTopics().iterator().next());
        }
        return kafkaTemplate;
    }

    @Bean
    public ConsumerFactory<String, String> autoCommitConsumerFactory() {
        StringDeserializer stringDeserializer = new StringDeserializer();
        return new DefaultKafkaConsumerFactory<>(autoCommitKafkaProperties.getConsumerConfig(),
                stringDeserializer, stringDeserializer);
    }


}
