package com.caiya.kafka.spring.boot.autoconfigure;

import com.caiya.kafka.springn.core.ConsumerFactory;
import com.caiya.kafka.springn.core.KafkaTemplate;
import com.caiya.kafka.springn.core.ProducerFactory;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KafkaAutoConfigurationTests.
 *
 * @author wangnan
 * @since 1.2.0, 2019/11/29
 **/
public class KafkaAutoConfigurationTests {


    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues("kafka.producer-config.bootstrap-servers=127.0.0.1:9093,127.0.0.1:9094",
                    "kafka.consumer-config.bootstrap-servers=127.0.0.1:9093,127.0.0.1:9094",
                    "kafka.topics=test")
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class));


    @Test
    public void defaultServiceBacksOff() {
        this.contextRunner.withUserConfiguration(KafkaAutoConfiguration.class)
                .run((context) -> {
                    assertThat(context).hasSingleBean(ProducerFactory.class);
                    assertThat(context.getBean(ProducerFactory.class)).isSameAs(
                            context.getBean(KafkaAutoConfiguration.class).producerFactory());
                    assertThat(context).hasSingleBean(KafkaTemplate.class);
                    assertThat(context).hasSingleBean(ConsumerFactory.class);
                    assertThat(context.getBean(ConsumerFactory.class)).isSameAs(
                            context.getBean(KafkaAutoConfiguration.class).consumerFactory());
                });
    }

    @Test
    public void serviceNameCanBeConfigured() {
        this.contextRunner.withPropertyValues("kafka.topics=test").run((context) -> {
            assertThat(context).hasSingleBean(KafkaProperties.class);
            assertThat(context.getBean(KafkaProperties.class).getTopics().iterator().next()).isEqualTo("test");
        });
    }


}
