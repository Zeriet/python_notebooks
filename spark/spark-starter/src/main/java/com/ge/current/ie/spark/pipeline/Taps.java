/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;

public class Taps {
    public static <T> Tap<JavaRDD<T>> logRDD(String prefix) {
        return (rdd) -> rdd.foreach((o) -> System.out.printf("%s%s\n", prefix, o));
    }

    public static <T, U> Tap<JavaPairRDD<T, U>> logPairRDD(String prefix) {
        return (rdd) -> rdd.foreach((o) -> System.out.printf("%s%s\n", prefix, o));
    }

    public static <T, U> Tap<JavaPairDStream<T, U>> logPairDStreamRDD(String prefix) {
        return (dstream) -> dstream.foreachRDD(rdd -> {
            rdd.foreach(t -> System.out.printf("%s%s\n", prefix, t));
        });
    }
}
