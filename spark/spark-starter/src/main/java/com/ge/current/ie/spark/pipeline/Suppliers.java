/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.ge.current.ie.spark.batch.BatchSupplier;
import com.ge.current.ie.spark.streaming.StreamSupplier;

public class Suppliers {
    public static BatchSupplier<JavaRDD<String>> textFile(String file) {
        return new BatchSupplier<JavaRDD<String>>() {
            @Override
            protected JavaRDD<String> get(JavaSparkContext sparkContext) {
                return sparkContext.textFile(file);
            }
        };
    }

    public static <T> StreamSupplier<JavaInputDStream<T>> receiverSupplier(Receiver<T> receiver) {
        return new StreamSupplier<JavaInputDStream<T>>() {
            @Override
            protected JavaInputDStream<T> get(JavaStreamingContext javaStreamingContext) {
                return javaStreamingContext.receiverStream(receiver);
            }
        };
    }
}
