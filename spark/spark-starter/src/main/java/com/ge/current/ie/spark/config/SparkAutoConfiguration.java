/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.config;

import java.util.Arrays;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import scala.Tuple2;

import com.ge.current.ie.spark.SparkConfAwareBeanProcessor;
import com.ge.current.ie.spark.batch.JavaSparkContextAwareProcessor;
import com.ge.current.ie.spark.streaming.JavaStreamingContextAwareProcessor;

@Configuration
@Import({ DelegatingPipelineConfiguration.class })
public class SparkAutoConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkAutoConfiguration.class);

    @Value("${ie.spark.app.name}")
    private String appName;

    @Value("${ie.spark.streaming.duration:10000}")
    private long duration;

    @Bean
    public SparkConf sparkConf(Environment environment) {
        SparkConf sc = new SparkConf().setAppName(appName);
        applyEnvironmentToSparkConf(environment, sc);
        return sc;
    }

    private void applyEnvironmentToSparkConf(Environment environment, SparkConf sc) {
        LOGGER.info("Applying environment...");
        if (environment instanceof AbstractEnvironment) {
            StreamSupport.stream(((AbstractEnvironment) environment).getPropertySources().spliterator(), true)
                .filter(ps -> (ps instanceof EnumerablePropertySource))
                .map(ps -> (EnumerablePropertySource) ps)
                .flatMap(ps -> Arrays.stream(ps.getPropertyNames())
                    .map(p ->  new Tuple2<>(p, ps.getProperty(p)))
                    .filter(t -> t._2() instanceof String)
                    .map(t -> new Tuple2<>(t._1(), (String) t._2())))
                .peek(t -> LOGGER.info("Setting property {} to value {} in spark conf", t._1(), t._2()))
                .forEach(t -> sc.set(t._1(), t._2()));
        }
    }

    @Bean
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public JavaStreamingContext javaStreamingContext(JavaSparkContext javaSparkContext) {
        return new JavaStreamingContext(javaSparkContext, new Duration(duration));
    }

    @Bean
    public JavaSparkContextAwareProcessor javaSparkContextAwareProcessor(JavaSparkContext javaSparkContext) {
        return new JavaSparkContextAwareProcessor(javaSparkContext);
    }

    @Bean
    public JavaStreamingContextAwareProcessor javaStreamingContextAwareProcessor(JavaStreamingContext javaStreamingContext) {
        return new JavaStreamingContextAwareProcessor(javaStreamingContext);
    }

    @Bean
    public SparkConfAwareBeanProcessor sparkConfAwareBeanProcessor(SparkConf sparkConf) {
        return new SparkConfAwareBeanProcessor(sparkConf);
    }

    @Bean
    public CommandLineRunner javaStreamingContextStarter(JavaStreamingContext javaStreamingContext) {
        return (args) -> {
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        };
    }
}
