package com.caiya.kafka.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.caiya.kafka.listener.MessageListenerContainer;
import com.caiya.kafka.spring.config.AbstractKafkaListenerContainerFactory;
import com.caiya.kafka.spring.config.ConcurrentKafkaListenerContainerFactory;
import com.caiya.kafka.spring.config.KafkaListenerEndpointRegistrar;
import com.caiya.kafka.spring.config.KafkaListenerEndpointRegistry;
import org.springframework.context.annotation.Import;

/**
 * Enable Kafka listener annotated endpoints that are created under the covers by a
 * {@link AbstractKafkaListenerContainerFactory
 * AbstractListenerContainerFactory}. To be used on
 * {@link org.springframework.context.annotation.Configuration Configuration} classes as
 * follows:
 * <p>
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig {
 * &#064;Bean
 * public ConcurrentKafkaListenerContainerFactory myKafkaListenerContainerFactory() {
 * ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
 * factory.setConsumerFactory(consumerFactory());
 * factory.setConcurrency(4);
 * return factory;
 * }
 * // other &#064;Bean definitions
 * }
 * </pre>
 * <p>
 * The {@code KafkaListenerContainerFactory} is responsible to create the listener
 * container for a particular endpoint. Typical implementations, as the
 * {@link ConcurrentKafkaListenerContainerFactory
 * ConcurrentKafkaListenerContainerFactory} used in the sample above, provides the necessary
 * configuration options that are supported by the underlying
 * {@link MessageListenerContainer
 * MessageListenerContainer}.
 * <p>
 * <p>
 * {@code @EnableKafka} enables detection of {@link KafkaListener} annotations on any
 * Spring-managed bean in the container. For example, given a class {@code MyService}:
 * <p>
 * <pre class="code">
 * package com.acme.foo;
 * <p>
 * public class MyService {
 * &#064;KafkaListener(containerFactory = "myKafkaListenerContainerFactory", topics = "myTopic")
 * public void process(String msg) {
 * // process incoming message
 * }
 * }
 * </pre>
 * <p>
 * The container factory to use is identified by the
 * {@link KafkaListener#containerFactory() containerFactory} attribute defining the name
 * of the {@code KafkaListenerContainerFactory} bean to use. When none is set a
 * {@code KafkaListenerContainerFactory} bean with name
 * {@code kafkaListenerContainerFactory} is assumed to be present.
 * <p>
 * <p>
 * the following configuration would ensure that every time a message is received from
 * topic "myQueue", {@code MyService.process()} is called with the content of the message:
 * <p>
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig {
 * &#064;Bean
 * public MyService myService() {
 * return new MyService();
 * }
 * <p>
 * // Kafka infrastructure setup
 * }
 * </pre>
 * <p>
 * Alternatively, if {@code MyService} were annotated with {@code @Component}, the
 * following configuration would ensure that its {@code @KafkaListener} annotated method
 * is invoked with a matching incoming message:
 * <p>
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafka
 * &#064;ComponentScan(basePackages = "com.acme.foo")
 * public class AppConfig {
 * }
 * </pre>
 * <p>
 * Note that the created containers are not registered with the application context but
 * can be easily located for management purposes using the
 * {@link KafkaListenerEndpointRegistry
 * KafkaListenerEndpointRegistry}.
 * <p>
 * <p>
 * Annotated methods can use a flexible signature; in particular, it is possible to use
 * the {@link org.springframework.messaging.Message Message} abstraction and related
 * annotations, see {@link KafkaListener} Javadoc for more details. For instance, the
 * following would inject the content of the message and the kafka partition
 * header:
 * <p>
 * <pre class="code">
 * &#064;KafkaListener(containerFactory = "myKafkaListenerContainerFactory", topics = "myTopic")
 * public void process(String msg, @Header("kafka_partition") int partition) {
 * // process incoming message
 * }
 * </pre>
 * <p>
 * These features are abstracted by the
 * {@link org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory
 * MessageHandlerMethodFactory} that is responsible to build the necessary invoker to
 * process the annotated method. By default,
 * {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory
 * DefaultMessageHandlerMethodFactory} is used.
 * <p>
 * <p>
 * When more control is desired, a {@code @Configuration} class may implement
 * {@link KafkaListenerConfigurer}. This allows access to the underlying
 * {@link KafkaListenerEndpointRegistrar
 * KafkaListenerEndpointRegistrar} instance. The following example demonstrates how to
 * specify an explicit default {@code KafkaListenerContainerFactory}
 * <p>
 * <pre class="code">
 * {
 * &#64;code
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig implements KafkaListenerConfigurer {
 * &#064;Override
 * public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
 * registrar.setContainerFactory(myKafkaListenerContainerFactory());
 * }
 * <p>
 * &#064;Bean
 * public KafkaListenerContainerFactory&lt;?, ?&gt; myKafkaListenerContainerFactory() {
 * // factory settings
 * }
 * <p>
 * &#064;Bean
 * public MyService myService() {
 * return new MyService();
 * }
 * }
 * }
 * </pre>
 * <p>
 * It is also possible to specify a custom
 * {@link KafkaListenerEndpointRegistry
 * KafkaListenerEndpointRegistry} in case you need more control on the way the containers
 * are created and managed. The example below also demonstrates how to customize the
 * {@code KafkaHandlerMethodFactory} to use with a custom
 * {@link org.springframework.validation.Validator Validator} so that payloads annotated
 * with {@link org.springframework.validation.annotation.Validated Validated} are first
 * validated against a custom {@code Validator}.
 * <p>
 * <pre class="code">
 * {
 * &#64;code
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig implements KafkaListenerConfigurer {
 * &#064;Override
 * public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
 * registrar.setEndpointRegistry(myKafkaListenerEndpointRegistry());
 * registrar.setMessageHandlerMethodFactory(myMessageHandlerMethodFactory);
 * }
 * <p>
 * &#064;Bean
 * public KafkaListenerEndpointRegistry myKafkaListenerEndpointRegistry() {
 * // registry configuration
 * }
 * <p>
 * &#064;Bean
 * public KafkaHandlerMethodFactory myMessageHandlerMethodFactory() {
 * DefaultKafkaHandlerMethodFactory factory = new DefaultKafkaHandlerMethodFactory();
 * factory.setValidator(new MyValidator());
 * return factory;
 * }
 * <p>
 * &#064;Bean
 * public MyService myService() {
 * return new MyService();
 * }
 * }
 * }
 * </pre>
 * <p>
 * Implementing {@code KafkaListenerConfigurer} also allows for fine-grained control over
 * endpoints registration via the {@code KafkaListenerEndpointRegistrar}. For example, the
 * following configures an extra endpoint:
 * <p>
 * <pre class="code">
 * {
 * &#64;code
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig implements KafkaListenerConfigurer {
 * &#064;Override
 * public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
 * SimpleKafkaListenerEndpoint myEndpoint = new SimpleKafkaListenerEndpoint();
 * // ... configure the endpoint
 * registrar.registerEndpoint(endpoint, anotherKafkaListenerContainerFactory());
 * }
 * <p>
 * &#064;Bean
 * public MyService myService() {
 * return new MyService();
 * }
 * <p>
 * &#064;Bean
 * public KafkaListenerContainerFactory&lt;?, ?&gt; anotherKafkaListenerContainerFactory() {
 * // ...
 * }
 * <p>
 * // Kafka infrastructure setup
 * }
 * }
 * </pre>
 * <p>
 * Note that all beans implementing {@code KafkaListenerConfigurer} will be detected and
 * invoked in a similar fashion. The example above can be translated in a regular bean
 * definition registered in the context in case you use the XML configuration.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @see KafkaListener
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListenerEndpointRegistrar
 * @see KafkaListenerEndpointRegistry
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaBootstrapConfiguration.class)
public @interface EnableKafka {
}
