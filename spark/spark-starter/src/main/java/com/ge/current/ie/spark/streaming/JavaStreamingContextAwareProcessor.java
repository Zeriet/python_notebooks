/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.streaming;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

public class JavaStreamingContextAwareProcessor implements BeanPostProcessor {

    private final JavaStreamingContext javaStreamingContext;

    public JavaStreamingContextAwareProcessor(JavaStreamingContext javaStreamingContext) {
        this.javaStreamingContext = javaStreamingContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof JavaStreamingContextAware) {
            ((JavaStreamingContextAware)bean).setJavaStreamingContext(javaStreamingContext);
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
