/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark;

import org.apache.spark.SparkConf;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

public class SparkConfAwareBeanProcessor implements BeanPostProcessor {
    private final SparkConf sparkConf;

    public SparkConfAwareBeanProcessor(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof SparkConfAware) {
            ((SparkConfAware)bean).setSparkConf(sparkConf);
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
