/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.batch;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

public class JavaSparkContextAwareProcessor implements BeanPostProcessor {

    private final JavaSparkContext javaSparkContext;

    public JavaSparkContextAwareProcessor(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof JavaSparkContextAware) {
            ((JavaSparkContextAware)bean).setJavaSparkContext(javaSparkContext);
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
