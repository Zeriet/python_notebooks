/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public final class Pipes {
    public static Pipe<JavaRDD<String>, JavaRDD<String>> splitter(String delim) {
        return (rdd) -> rdd.flatMap(s -> Arrays.asList(s.split(delim)));
    }

    public static <T> Pipe<JavaRDD<T>, JavaPairRDD<T, Integer>> toTupleRDD() {
        return (rdd) -> rdd.mapToPair(o -> new Tuple2<T, Integer>(o, 1));
    }

    public static <T> Pipe<JavaPairRDD<T, Integer>, JavaPairRDD<T, Integer>> sumByKey() {
        return (rdd) -> rdd.reduceByKey((a, b) -> a + b);
    }

    public static Pipe<JavaRDD<String>, JavaRDD<String>> trim() {
        return (rdd) -> rdd.map(s -> s.trim());
    }
}
