package com.caiya.kafka.spring.configuration;

import com.caiya.kafka.*;
import com.caiya.kafka.spring.annotation.EnableKafka;
import com.caiya.kafka.spring.component.KafkaProperties;
import com.caiya.kafka.spring.config.ConcurrentKafkaListenerContainerFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka相关配置.
 *
 * @author wangnan
 * @since 1.0
 */
@Configuration
@EnableKafka
public class KafkaConfiguration {

    private final KafkaProperties kafkaProperties;

    @Autowired
    public KafkaConfiguration(KafkaProperties kafkaProperties) {
        replaceConfigKey(kafkaProperties);
        this.kafkaProperties = kafkaProperties;
    }

    private void replaceConfigKey(KafkaProperties kafkaProperties) {
        if (kafkaProperties.getProducerConfig() != null) {
            Map<String, Object> configMap = new HashMap<>();
            for(Map.Entry<String, Object> configEntry : kafkaProperties.getProducerConfig().entrySet()){
                configMap.put(configEntry.getKey().replaceAll("-", "."), configEntry.getValue());
            }
            kafkaProperties.setProducerConfig(configMap);
        }
        if (kafkaProperties.getConsumerConfig() != null) {
            Map<String, Object> configMap = new HashMap<>();
            for(Map.Entry<String, Object> configEntry : kafkaProperties.getConsumerConfig().entrySet()){
                configMap.put(configEntry.getKey().replaceAll("-", "."), configEntry.getValue());
            }
            kafkaProperties.setConsumerConfig(configMap);
        }
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        StringSerializer stringSerializer = new StringSerializer();
        return new DefaultKafkaProducerFactory<>(kafkaProperties.getProducerConfig(),
                stringSerializer, stringSerializer);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(kafkaProperties.getCanalTopic());
        return kafkaTemplate;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.getConsumerConfig(),
                new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


}
