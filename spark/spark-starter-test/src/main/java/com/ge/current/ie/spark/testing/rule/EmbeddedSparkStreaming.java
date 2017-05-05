/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.testing.rule;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.junit.rules.ExternalResource;

public class EmbeddedSparkStreaming extends ExternalResource {
    private JavaStreamingContext jssc;
    private String master;
    private Duration duration;

    public EmbeddedSparkStreaming() {
        this("local[*]", 100L);
    }

    public EmbeddedSparkStreaming(String master, long duration) {
        this.master = master;
        this.duration = new Duration(duration);
    }

    @Override
    protected void before() throws Throwable {
        jssc = new JavaStreamingContext(master, EmbeddedSparkStreaming.class.getName(), duration);
    }

    @Override
    protected void after() {
        jssc.stop();
    }

    public JavaStreamingContext getContext() {
        return jssc;
    }

    public <T> JavaReceiverInputDStream<T> receiverStream(Receiver<T> receiver) {
        return jssc.receiverStream(receiver);
    }

    public void awaitTermination(long timeout) {
        jssc.awaitTerminationOrTimeout(timeout);
    }

    public void start() {
        jssc.start();
    }
}
