package com.caiya.kafka.spring.listener.adaptor;

import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the
 * underlying handler.
 *
 * @author Gary Russell
 *
 */
public class HandlerAdapter {

    private final InvocableHandlerMethod invokerHandlerMethod;

    private final DelegatingInvocableHandler delegatingHandler;

    public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
        this.invokerHandlerMethod = invokerHandlerMethod;
        this.delegatingHandler = null;
    }

    public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
        this.invokerHandlerMethod = null;
        this.delegatingHandler = delegatingHandler;
    }

    public Object invoke(Message<?> message, Object... providedArgs) throws Exception { //NOSONAR
        if (this.invokerHandlerMethod != null) {
            return this.invokerHandlerMethod.invoke(message, providedArgs);
        }
        else if (this.delegatingHandler.hasDefaultHandler()) {
            // Needed to avoid returning raw Message which matches Object
            Object[] args = new Object[providedArgs.length + 1];
            args[0] = message.getPayload();
            System.arraycopy(providedArgs, 0, args, 1, providedArgs.length);
            return this.delegatingHandler.invoke(message, args);
        }
        else {
            return this.delegatingHandler.invoke(message, providedArgs);
        }
    }

    public String getMethodAsString(Object payload) {
        if (this.invokerHandlerMethod != null) {
            return this.invokerHandlerMethod.getMethod().toGenericString();
        }
        else {
            return this.delegatingHandler.getMethodNameFor(payload);
        }
    }

    public Object getBean() {
        if (this.invokerHandlerMethod != null) {
            return this.invokerHandlerMethod.getBean();
        }
        else {
            return this.delegatingHandler.getBean();
        }
    }

}
