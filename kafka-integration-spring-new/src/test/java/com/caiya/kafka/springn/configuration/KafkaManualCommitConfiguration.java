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
 * Kafka相关配置（手动提交）.
 *
 * @author wangnan
 * @since 1.0
 */
@Configuration
public class KafkaManualCommitConfiguration {

    @Resource
    private KafkaProperties manualCommitKafkaProperties;

    @Bean
    @ConfigurationProperties(prefix = "kafka.test-manual")
    public KafkaProperties manualCommitKafkaProperties() {
        return new KafkaProperties();
    }

    @Bean
    public ProducerFactory<String, String> manualCommitProducerFactory() {
        StringSerializer stringSerializer = new StringSerializer();
        return new DefaultKafkaProducerFactory<>(manualCommitKafkaProperties.getProducerConfig(),
                stringSerializer, stringSerializer);
    }

    @Bean
    public KafkaTemplate<String, String> manualCommitKafkaTemplate(ProducerFactory<String, String> manualCommitProducerFactory) {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(manualCommitProducerFactory);
        if (!CollectionUtils.isEmpty(manualCommitKafkaProperties.getTopics())) {
            // use the first one as the default topic
            kafkaTemplate.setDefaultTopic(manualCommitKafkaProperties.getTopics().iterator().next());
        }
        return kafkaTemplate;
    }

    @Bean
    public ConsumerFactory<String, String> manualCommitConsumerFactory() {
        StringDeserializer stringDeserializer = new StringDeserializer();
        return new DefaultKafkaConsumerFactory<>(manualCommitKafkaProperties.getConsumerConfig(),
                stringDeserializer, stringDeserializer);
    }


}
