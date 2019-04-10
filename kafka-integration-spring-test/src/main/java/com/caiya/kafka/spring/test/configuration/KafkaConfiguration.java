package com.caiya.kafka.spring.test.configuration;

import com.caiya.kafka.*;
import com.caiya.kafka.spring.annotation.EnableKafka;
import com.caiya.kafka.spring.config.ConcurrentKafkaListenerContainerFactory;
import com.caiya.kafka.spring.test.component.KafkaProperties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * Kafka相关配置.
 *
 * @author wangnan
 * @since 1.0
 */
@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Resource
    private KafkaProperties kafkaProperties;

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
