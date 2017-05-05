/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.testing.rule;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.junit.rules.ExternalResource;

public class EmbeddedSpark extends ExternalResource {
    private final SparkConf sparkConf;
    private JavaSparkContext jsc;
    private String master;
    private Duration duration;

    public EmbeddedSpark() {
        this("local[*]", new SparkConf());
    }

    public EmbeddedSpark(String master, SparkConf sc) {
        this.master = master;
        this.sparkConf = sc;
    }

    @Override
    protected void before() throws Throwable {
        jsc = new JavaSparkContext(master, EmbeddedSpark.class.getName(), sparkConf);
    }

    @Override
    protected void after() {
        jsc.stop();
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public JavaSparkContext getContext() {
        return jsc;
    }

    public <T> JavaRDD<T> parallelize(List<T> rdd) {
        return jsc.parallelize(rdd);
    }

    public <T> JavaRDD<T> parallelize(T... rdd) {
        return parallelize(Arrays.asList(rdd));
    }

    public <T> Broadcast<T> broadcast(T value) {
        return jsc.broadcast(value);
    }
}
