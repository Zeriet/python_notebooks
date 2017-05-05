/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;

/**
 * Provides {@link java.util.Optional} return values from spark configuration.
 */
public class SparkConfFunctions {

    public static Optional<String> get(SparkConf sparkConf, String key) {
        return Optional.ofNullable(sparkConf.get(key))
                .filter(s -> !s.isEmpty());
    }

    private static final Function<String, String> PARSE_FUNCTION = Function.identity();

    public static List<String> getList(SparkConf sparkConf, String key) {
        return getList(sparkConf, key, ",");
    }

    public static List<String> getList(SparkConf sparkConf, String key, String sep) {
        return getList(sparkConf, key, sep, PARSE_FUNCTION);
    }

    public static <T> List<T> getList(SparkConf sparkConf, String key, String sep, Function<String, T> parseFunction) {
        String val = sparkConf.get(key);
        if (val == null || val.isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.stream(val.split(sep))
                .map(parseFunction)
                .collect(Collectors.toList());
    }

    public static Optional<Integer> getInt(SparkConf sparkConf, String str) {
        return get(sparkConf, str)
                .map(s -> Integer.parseInt(s));
    }

    public static Supplier<ConfigurationException> supplyMissingConfigException(String property) {
        return () -> new ConfigurationException("Missing property %s inside Spark config.", property);
    }

    public static Optional<Boolean> getBool(SparkConf sparkConf, String prop) {
        return get(sparkConf, prop)
                .map(Boolean::parseBoolean);
    }
}
