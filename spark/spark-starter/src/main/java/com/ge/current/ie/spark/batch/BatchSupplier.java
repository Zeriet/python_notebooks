/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.batch;

import java.util.function.Supplier;

import org.apache.spark.api.java.JavaSparkContext;

public abstract class BatchSupplier<T> implements Supplier<T>, JavaSparkContextAware {

    private JavaSparkContext javaSparkContext;

    @Override
    public T get() {
        return get(javaSparkContext);
    }

    protected abstract T get(JavaSparkContext sparkContext);

    @Override
    public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
    }
}
