/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.streaming;

import java.util.function.Supplier;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

public abstract class StreamSupplier<T> implements Supplier<T>, JavaStreamingContextAware {
    private JavaStreamingContext javaStreamingContext;

    @Override
    public T get() {
        return get(javaStreamingContext);
    }

    protected abstract T get(JavaStreamingContext javaStreamingContext);

    @Override
    public void setJavaStreamingContext(JavaStreamingContext javaStreamingContext) {
        this.javaStreamingContext = javaStreamingContext;
    }
}
