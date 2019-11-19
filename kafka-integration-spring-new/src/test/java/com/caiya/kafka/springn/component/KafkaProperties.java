package com.caiya.kafka.springn.component;

import org.springframework.beans.factory.InitializingBean;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka的属性配置类.
 *
 * @author wangnan
 * @since 1.0
 */
public class KafkaProperties implements InitializingBean {

    private Map<String, Object> producerConfig;

    private Map<String, Object> consumerConfig;

    private String[] topics;


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

    public Collection<String> getTopics() {
        return Arrays.asList(topics);
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        filterKafkaProperties(this);
    }

    /**
     * 配置转换，将"-"替换为"."（yml文件不允许下划线_和.等符号）
     */
    public static void filterKafkaProperties(KafkaProperties kafkaProperties) {
        if (kafkaProperties.getProducerConfig() != null) {
            Map<String, Object> configMap = new HashMap<>();
            for (Map.Entry<String, Object> configEntry : kafkaProperties.getProducerConfig().entrySet()) {
                configMap.put(configEntry.getKey().replaceAll("-", "."), configEntry.getValue());
            }
            kafkaProperties.setProducerConfig(configMap);
        }
        if (kafkaProperties.getConsumerConfig() != null) {
            Map<String, Object> configMap = new HashMap<>();
            for (Map.Entry<String, Object> configEntry : kafkaProperties.getConsumerConfig().entrySet()) {
                configMap.put(configEntry.getKey().replaceAll("-", "."), configEntry.getValue());
            }
            kafkaProperties.setConsumerConfig(configMap);
        }
    }

}
