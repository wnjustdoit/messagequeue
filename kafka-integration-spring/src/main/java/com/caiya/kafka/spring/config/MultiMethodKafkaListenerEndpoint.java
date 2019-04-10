package com.caiya.kafka.spring.config;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.caiya.kafka.spring.listener.adaptor.DelegatingInvocableHandler;
import com.caiya.kafka.spring.listener.adaptor.HandlerAdapter;
import com.caiya.kafka.spring.listener.adaptor.MessagingMessageListenerAdapter;
import com.caiya.kafka.spring.annotation.KafkaHandler;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * The {@link MethodKafkaListenerEndpoint} extension for several POJO methods
 * based on the {@link KafkaHandler}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @see KafkaHandler
 * @see DelegatingInvocableHandler
 */
public class MultiMethodKafkaListenerEndpoint<K, V> extends MethodKafkaListenerEndpoint<K, V> {

    private final List<Method> methods;

    private final Method defaultMethod;

    /**
     * Construct an instance for the provided methods and bean with no default method.
     * @param methods the methods.
     * @param bean the bean.
     */
    public MultiMethodKafkaListenerEndpoint(List<Method> methods, Object bean) {
        this(methods, null, bean);
    }

    /**
     * Construct an instance for the provided methods, default method and bean.
     * @param methods the methods.
     * @param defaultMethod the default method.
     * @param bean the bean.
     * @since 2.1.3
     */
    public MultiMethodKafkaListenerEndpoint(List<Method> methods, Method defaultMethod, Object bean) {
        this.methods = methods;
        this.defaultMethod = defaultMethod;
        setBean(bean);
    }

    @Override
    protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter<K, V> messageListener) {
        List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<InvocableHandlerMethod>();
        InvocableHandlerMethod defaultHandler = null;
        for (Method method : this.methods) {
            InvocableHandlerMethod handler = getMessageHandlerMethodFactory()
                    .createInvocableHandlerMethod(getBean(), method);
            invocableHandlerMethods.add(handler);
            if (method.equals(this.defaultMethod)) {
                defaultHandler = handler;
            }
        }
        DelegatingInvocableHandler delegatingHandler = new DelegatingInvocableHandler(invocableHandlerMethods,
                defaultHandler, getBean(), getResolver(), getBeanExpressionContext());
        return new HandlerAdapter(delegatingHandler);
    }

}
