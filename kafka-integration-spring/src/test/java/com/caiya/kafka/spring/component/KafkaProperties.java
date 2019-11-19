package com.caiya.kafka.spring.component;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Kafka的属性配置类.
 *
 * @author wangnan
 * @since 1.0
 */
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private Map<String, Object> producerConfig;

    private Map<String, Object> consumerConfig;

    private String canalTopic;


    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(Map<String, Object> producerConfig) {
        this.producerConfig = producerConfig;
    }

    public Map<String, Object> getConsumerConfig() {
        return consumerConfig;
    }

    public void setConsumerConfig(Map<String, Object> consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public String getCanalTopic() {
        return canalTopic;
    }

    public void setCanalTopic(String canalTopic) {
        this.canalTopic = canalTopic;
    }
}
